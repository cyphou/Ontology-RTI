<#
.SYNOPSIS
    Deploy a Fabric Operations Agent for IT Asset real-time monitoring.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$EventhouseId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$AgentName = "ITAssetOpsAgent"
)

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying Operations Agent: $AgentName ===" -ForegroundColor Cyan

# ── Auto-detect Eventhouse/KQL Database if not provided ─────────────────────
if (-not $EventhouseId -or -not $KqlDatabaseId) {
    Write-Host "[Auto-detect] Looking up Eventhouse and KQL Database in workspace..." -ForegroundColor Yellow
    try {
        $kqlDbs = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/kqlDatabases" -Headers $headers).value
        if ($kqlDbs -and $kqlDbs.Count -gt 0) {
            $kqlDb = $kqlDbs | Select-Object -First 1
            if (-not $KqlDatabaseId) { $KqlDatabaseId = $kqlDb.id; Write-Host "  Found KQL Database: $($kqlDb.displayName) ($KqlDatabaseId)" -ForegroundColor Green }
            if (-not $EventhouseId -and $kqlDb.properties -and $kqlDb.properties.parentEventhouseItemId) {
                $EventhouseId = $kqlDb.properties.parentEventhouseItemId
                Write-Host "  Found Eventhouse: $EventhouseId" -ForegroundColor Green
            }
        }
        if (-not $EventhouseId) {
            $ehs = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/eventhouses" -Headers $headers).value
            if ($ehs -and $ehs.Count -gt 0) { $EventhouseId = $ehs[0].id; Write-Host "  Found Eventhouse: $($ehs[0].displayName) ($EventhouseId)" -ForegroundColor Green }
        }
    } catch {
        Write-Host "  [WARN] Auto-detect failed: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

$allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
$existing = $allItems | Where-Object { $_.displayName -eq $AgentName }
if ($existing) { $agentId = $existing.id } else {
    $createBody = @{displayName=$AgentName;type="DataAgent";description="Operations monitoring agent for IT infrastructure"} | ConvertTo-Json -Depth 5
    try { $resp = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
        if ($resp.StatusCode -eq 201) { $agentId = ($resp.Content|ConvertFrom-Json).id }
        elseif ($resp.StatusCode -eq 202) { $opUrl=$resp.Headers['Location']; do { Start-Sleep -Seconds 3; $poll=Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled')); $allI=(Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $agentId=($allI|Where-Object{$_.displayName -eq $AgentName}).id }
    } catch { exit 1 }
}
if (-not $agentId) { exit 1 }

$fullInstructions = @"
== IT ASSET OPERATIONS AGENT ==

Goal 1: Server Health Monitoring
- Track CPU, Memory, Disk IOPS, Network per server in real-time.
- Alert thresholds: CPU >90% Critical, >80% Warning. Memory >85% Warning.
- Disk IOPS >15000 = storage bottleneck. >20000 = critical.
- Identify servers with sustained high utilization (>80% for >30 min).

Goal 2: Application Performance
- Monitor response time per application. Alert when >2x baseline.
- Track error rate: >1% = degraded, >5% = critical.
- Active connection count vs capacity. Alert at >80% utilization.
- SLA compliance: compare actual vs target response times.

Goal 3: Network Infrastructure
- Bandwidth utilization: Warning >70%, Critical >90%.
- Packet loss: >0.1% = degraded, >1% = critical.
- Latency: >30ms intra-DC = high, >100ms cross-DC = critical.
- Track port utilization and error counts per device.

Goal 4: Incident Management
- Track open incidents by severity and age.
- Calculate MTTR (Mean Time To Resolve) by severity and type.
- Identify servers with >2 incidents in 7 days = chronic issue.
- Escalate P1/Critical incidents unresolved after 1 hour.
- Track affected user count per incident for impact prioritization.

Goal 5: Capacity & License Planning
- Identify servers approaching capacity limits (CPU/Memory/Storage).
- License expiration tracking: alert 30 days before expiry.
- Track license seat utilization (used vs purchased).
- Forecast capacity needs based on utilization trends.

== KQL TABLES ==
ServerMetric (ServerId, RackId, DataCenterId, Timestamp, CPUPercent, MemoryPercent, DiskIOPS, NetworkMbps, QualityFlag, IsAnomaly)
InfraAlert (AlertId, ServerId, RackId, DataCenterId, Timestamp, AlertType, Severity, MetricValue, ThresholdValue, Message, IsAcknowledged)
ApplicationHealth (AppId, ServerId, Timestamp, ResponseTimeMs, ErrorRate, RequestsPerSec, ActiveConnections, HealthStatus)
NetworkMetric (DeviceId, DataCenterId, Timestamp, BandwidthUtilPct, PacketLossPct, LatencyMs, ActivePorts, ErrorCount, Status)
IncidentMetric (IncidentId, ServerId, Severity, IncidentType, Timestamp, ResolvedTimestamp, DurationHours, AffectedUsers, RootCause, Status)

Always include: timestamp, affected server/datacenter, severity, recommended action. Use units.
"@ -replace "`r`n", "\n" -replace "`n", "\n" -replace '"', '\"'

$dataAgentB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"}'))
$stageB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json","aiInstructions":"' + $fullInstructions + '"}'))

$updateBody = @{definition=@{parts=@(@{path="Files/Config/data_agent.json";payload=$dataAgentB64;payloadType="InlineBase64"},@{path="Files/Config/draft/stage_config.json";payload=$stageB64;payloadType="InlineBase64"})}} | ConvertTo-Json -Depth 10
try { $r = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items/$agentId/updateDefinition" -Method POST -Headers $headers -Body $updateBody -UseBasicParsing; if ($r.StatusCode -in @(200,202)) { Write-Host "[OK] Operations Agent configured." -ForegroundColor Green } } catch { Write-Host "[WARN] $($_.Exception.Message)" -ForegroundColor Yellow }

Write-Host "=== Operations Agent Complete ===" -ForegroundColor Cyan
