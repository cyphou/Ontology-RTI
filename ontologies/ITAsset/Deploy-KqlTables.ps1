<#
.SYNOPSIS
    Create KQL tables and ingest data for the IT Asset domain.
.DESCRIPTION
    Creates 5 KQL tables for IT infrastructure telemetry:
      - ServerMetric        (enriched from SensorTelemetry.csv with rack/datacenter context)
      - InfraAlert          (alerts from FactAlert.csv with server/rack context)
      - ApplicationHealth   (application response time, error rate, throughput)
      - NetworkMetric       (network device bandwidth, packet loss, latency)
      - IncidentMetric      (incident timeline, resolution, impact tracking)
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$EventhouseId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$QueryServiceUri,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseName,
    [Parameter(Mandatory=$false)] [string]$DataFolder
)

$ErrorActionPreference = "Stop"
if (-not $DataFolder) { $DataFolder = Join-Path (Split-Path -Parent $PSScriptRoot) "data" }

Write-Host "=== Deploying IT Asset KQL Tables ===" -ForegroundColor Cyan

$fabricToken = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$fabricHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

if (-not $EventhouseId -or -not $KqlDatabaseId -or -not $QueryServiceUri) {
    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $fabricHeaders).value
    if (-not $KqlDatabaseId) { $kqlDb = $allItems | Where-Object { $_.type -eq 'KQLDatabase' } | Select-Object -First 1; if ($kqlDb) { $KqlDatabaseId = $kqlDb.id } else { exit 1 } }
    if (-not $EventhouseId) { $eh = $allItems | Where-Object { $_.type -eq 'Eventhouse' } | Select-Object -First 1; if ($eh) { $EventhouseId = $eh.id } }
    if (-not $QueryServiceUri -and $EventhouseId) { $ehD = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/eventhouses/$EventhouseId" -Headers $fabricHeaders; $QueryServiceUri = $ehD.properties.queryServiceUri }
}
if (-not $QueryServiceUri) { exit 1 }
if (-not $KqlDatabaseName) { $kqlDbDetails = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/kqlDatabases/$KqlDatabaseId" -Headers $fabricHeaders; $KqlDatabaseName = $kqlDbDetails.displayName }

$kustoToken = $null
foreach ($resource in @($QueryServiceUri, "https://kusto.kusto.windows.net", "https://api.fabric.microsoft.com")) {
    try { $kustoToken = (Get-AzAccessToken -ResourceUrl $resource).Token; break } catch {}
}
if (-not $kustoToken) { exit 1 }

function Invoke-KustoMgmt {
    param([string]$Command, [string]$Description)
    if ($Description) { Write-Host "  $Description" -ForegroundColor Gray }
    $body = @{ db = $KqlDatabaseName; csl = $Command } | ConvertTo-Json -Depth 2
    $h = @{ "Authorization" = "Bearer $kustoToken"; "Content-Type" = "application/json; charset=utf-8" }
    for ($a = 1; $a -le 3; $a++) {
        try { return Invoke-RestMethod -Method Post -Uri "$QueryServiceUri/v1/rest/mgmt" -Headers $h -Body ([System.Text.Encoding]::UTF8.GetBytes($body)) -ContentType "application/json; charset=utf-8" }
        catch { if ($a -lt 3) { Start-Sleep -Seconds (10 * $a) } else { throw } }
    }
}

for ($w = 1; $w -le 6; $w++) { try { Invoke-KustoMgmt -Command ".show database" | Out-Null; break } catch { Start-Sleep -Seconds 15 } }

# Ã¢â€â‚¬Ã¢â€â‚¬ CREATE TABLES Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
Write-Host "`n[Step 1] Creating KQL tables..." -ForegroundColor Cyan

$tables = @(
    @{ Name = "ServerMetric"; Schema = "(ServerId:string, RackId:string, DataCenterId:string, Timestamp:datetime, CPUPercent:real, MemoryPercent:real, DiskIOPS:int, NetworkMbps:real, QualityFlag:string, IsAnomaly:bool)" },
    @{ Name = "InfraAlert"; Schema = "(AlertId:string, ServerId:string, RackId:string, DataCenterId:string, Timestamp:datetime, AlertType:string, Severity:string, MetricValue:real, ThresholdValue:real, Message:string, IsAcknowledged:bool)" },
    @{ Name = "ApplicationHealth"; Schema = "(AppId:string, ServerId:string, Timestamp:datetime, ResponseTimeMs:real, ErrorRate:real, RequestsPerSec:real, ActiveConnections:int, HealthStatus:string)" },
    @{ Name = "NetworkMetric"; Schema = "(DeviceId:string, DataCenterId:string, Timestamp:datetime, BandwidthUtilPct:real, PacketLossPct:real, LatencyMs:real, ActivePorts:int, ErrorCount:int, Status:string)" },
    @{ Name = "IncidentMetric"; Schema = "(IncidentId:string, ServerId:string, Severity:string, IncidentType:string, Timestamp:datetime, ResolvedTimestamp:datetime, DurationHours:real, AffectedUsers:int, RootCause:string, Status:string)" }
)

foreach ($t in $tables) {
    try { Invoke-KustoMgmt -Command ".create-merge table $($t.Name) $($t.Schema)" -Description "Creating $($t.Name)..." | Out-Null; Write-Host "  [OK] $($t.Name)" -ForegroundColor Green }
    catch { Write-Host "  [WARN] $($t.Name): $_" -ForegroundColor Yellow }
}

# Ã¢â€â‚¬Ã¢â€â‚¬ Enable streaming ingestion policies Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
Write-Host "`n[Step 1b] Enabling streaming ingestion policies..." -ForegroundColor Cyan
foreach ($t in $tables) {
    try { Invoke-KustoMgmt -Command ".alter table $($t.Name) policy streamingingestion '{`"IsEnabled`": true}'" -Description "Streaming on $($t.Name)..." | Out-Null; Write-Host "  [OK] $($t.Name) streaming enabled" -ForegroundColor Green }
    catch { Write-Host "  [WARN] $($t.Name) streaming policy: $_" -ForegroundColor Yellow }
}

# Ã¢â€â‚¬Ã¢â€â‚¬ ENRICH SensorTelemetry Ã¢â€ â€™ ServerMetric Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
Write-Host "`n[Step 2] Enriching SensorTelemetry Ã¢â€ â€™ ServerMetric..." -ForegroundColor Cyan

# IT Asset telemetry has: ReadingId,ServerId,Timestamp,CPUPercent,MemoryPercent,DiskIOPS,NetworkMbps,QualityFlag,IsAnomaly
$serverLookup = @{}
$serverCsvPath = Join-Path $DataFolder "DimServer.csv"
if (Test-Path $serverCsvPath) {
    Import-Csv -Path $serverCsvPath | ForEach-Object { $serverLookup[$_.ServerId] = @{ RackId = $_.RackId; DataCenterId = $_.DataCenterId } }
}

$telemetry = Import-Csv -Path (Join-Path $DataFolder "SensorTelemetry.csv")
$lines = @()
foreach ($row in $telemetry) {
    $srv = $serverLookup[$row.ServerId]
    $rid = if ($srv) { $srv.RackId } else { "UNKNOWN" }
    $did = if ($srv) { $srv.DataCenterId } else { "UNKNOWN" }
    $lines += "$($row.ServerId),$rid,$did,$($row.Timestamp),$($row.CPUPercent),$($row.MemoryPercent),$($row.DiskIOPS),$($row.NetworkMbps),$($row.QualityFlag),$($row.IsAnomaly)"
}
for ($i = 0; $i -lt $lines.Count; $i += 50) {
    $batch = $lines[$i..([Math]::Min($i + 49, $lines.Count - 1))]
    try { Invoke-KustoMgmt -Command ".ingest inline into table ServerMetric with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch { Write-Warning "Batch ingest failed (non-fatal): $(    try { Invoke-KustoMgmt -Command ".ingest inline into table ServerMetric with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch { Write-Warning "Batch ingest failed (non-fatal): $($_.Exception.Message)" }.Exception.Message)" }
}
Write-Host "  [OK] ServerMetric ($($lines.Count) rows)" -ForegroundColor Green

# Ã¢â€â‚¬Ã¢â€â‚¬ INGEST InfraAlert Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
Write-Host "`n[Step 3] Ingesting InfraAlert sample data..." -ForegroundColor Cyan

$alertData = @(
    "ALR-001,SRV-005,RK-003,DC-002,2024-10-01T08:30:00,HighCPU,Critical,98.5,90.0,Server SRV-005 CPU at 98.5% - disk I/O saturation,false"
    "ALR-002,SRV-003,RK-002,DC-001,2024-10-01T09:15:00,HighMemory,High,88.0,85.0,Server SRV-003 memory utilization high,false"
    "ALR-003,SRV-007,RK-004,DC-002,2024-10-01T10:00:00,HardwareFailure,Critical,0.0,1.0,GPU failure detected on SRV-007,false"
    "ALR-004,SRV-001,RK-001,DC-001,2024-10-01T11:30:00,HighDiskIO,Medium,15000,12000,Disk IOPS exceeding baseline on SRV-001,true"
    "ALR-005,SRV-009,RK-005,DC-003,2024-10-01T12:45:00,HighCPU,High,92.0,90.0,Batch processing driving CPU high on SRV-009,false"
    "ALR-006,SRV-002,RK-001,DC-001,2024-10-01T14:00:00,NetworkLatency,Medium,45.0,30.0,Network latency elevated to SRV-002,true"
    "ALR-007,SRV-011,RK-006,DC-003,2024-10-01T15:15:00,DiskSpaceLow,High,92.0,85.0,Disk usage at 92% on SRV-011,false"
    "ALR-008,SRV-004,RK-002,DC-001,2024-10-01T16:30:00,ServiceDown,Critical,0.0,1.0,Database service unresponsive on SRV-004,false"
    "ALR-009,SRV-008,RK-004,DC-002,2024-10-02T08:00:00,HighMemory,High,87.5,85.0,Memory pressure on AI training server,false"
    "ALR-010,SRV-012,RK-006,DC-003,2024-10-02T09:30:00,CertExpiring,Low,7.0,30.0,SSL certificate expires in 7 days,true"
    "ALR-011,SRV-005,RK-003,DC-002,2024-10-02T10:15:00,HighCPU,Critical,96.0,90.0,Repeat CPU overload on SRV-005,false"
    "ALR-012,SRV-013,RK-007,DC-004,2024-10-02T11:00:00,HighDiskIO,Medium,14000,12000,Storage array I/O elevated,true"
    "ALR-013,SRV-006,RK-003,DC-002,2024-10-02T13:00:00,HighCPU,High,91.0,90.0,Web server CPU above threshold,false"
    "ALR-014,SRV-010,RK-005,DC-003,2024-10-02T14:30:00,NetworkLatency,High,52.0,30.0,Cross-DC replication latency high,false"
    "ALR-015,SRV-015,RK-008,DC-005,2024-10-02T16:00:00,HighMemory,Medium,83.0,80.0,Edge server memory pressure rising,true"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table InfraAlert with (format='csv') <|`n$($alertData -join "`n")" -Description "Ingesting 15 InfraAlert rows..." | Out-Null; Write-Host "  [OK] InfraAlert (15 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] InfraAlert: $_" -ForegroundColor Yellow }

# Ã¢â€â‚¬Ã¢â€â‚¬ INGEST ApplicationHealth Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
Write-Host "`n[Step 4] Ingesting ApplicationHealth sample data..." -ForegroundColor Cyan

$appData = @(
    "APP-001,SRV-001,2024-10-01T08:00:00,45.2,0.002,350,120,Healthy"
    "APP-001,SRV-001,2024-10-01T12:00:00,52.8,0.003,480,185,Healthy"
    "APP-001,SRV-001,2024-10-01T16:00:00,38.5,0.001,280,95,Healthy"
    "APP-002,SRV-002,2024-10-01T08:00:00,120.5,0.008,150,45,Degraded"
    "APP-002,SRV-002,2024-10-01T12:00:00,85.0,0.004,220,68,Healthy"
    "APP-003,SRV-003,2024-10-01T08:00:00,25.0,0.001,800,320,Healthy"
    "APP-003,SRV-003,2024-10-01T12:00:00,28.5,0.001,920,380,Healthy"
    "APP-004,SRV-004,2024-10-01T08:00:00,15.0,0.000,1200,500,Healthy"
    "APP-004,SRV-004,2024-10-01T16:00:00,0.0,1.000,0,0,Down"
    "APP-005,SRV-005,2024-10-01T08:00:00,200.5,0.025,80,25,Critical"
    "APP-005,SRV-005,2024-10-01T12:00:00,180.0,0.018,95,30,Degraded"
    "APP-006,SRV-006,2024-10-01T08:00:00,55.0,0.005,250,100,Healthy"
    "APP-006,SRV-006,2024-10-01T12:00:00,62.0,0.006,280,115,Healthy"
    "APP-007,SRV-008,2024-10-01T08:00:00,350.0,0.012,50,15,Degraded"
    "APP-007,SRV-008,2024-10-01T12:00:00,320.0,0.010,55,18,Degraded"
    "APP-008,SRV-009,2024-10-01T08:00:00,30.0,0.002,600,200,Healthy"
    "APP-008,SRV-009,2024-10-01T12:00:00,35.0,0.003,650,220,Healthy"
    "APP-009,SRV-010,2024-10-01T08:00:00,42.0,0.002,400,150,Healthy"
    "APP-009,SRV-010,2024-10-01T12:00:00,48.0,0.003,450,170,Healthy"
    "APP-010,SRV-012,2024-10-01T08:00:00,18.0,0.001,900,350,Healthy"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table ApplicationHealth with (format='csv') <|`n$($appData -join "`n")" -Description "Ingesting 20 ApplicationHealth rows..." | Out-Null; Write-Host "  [OK] ApplicationHealth (20 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] ApplicationHealth: $_" -ForegroundColor Yellow }

# Ã¢â€â‚¬Ã¢â€â‚¬ INGEST NetworkMetric Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
Write-Host "`n[Step 5] Ingesting NetworkMetric sample data..." -ForegroundColor Cyan

$netData = @(
    "ND-001,DC-001,2024-10-01T08:00:00,45.0,0.01,2.5,24,0,Active"
    "ND-001,DC-001,2024-10-01T12:00:00,72.0,0.02,3.0,24,2,Active"
    "ND-001,DC-001,2024-10-01T16:00:00,38.0,0.01,2.2,22,0,Active"
    "ND-002,DC-001,2024-10-01T08:00:00,55.0,0.05,1.8,48,1,Active"
    "ND-002,DC-001,2024-10-01T12:00:00,82.0,0.08,2.5,48,5,Warning"
    "ND-003,DC-002,2024-10-01T08:00:00,30.0,0.00,1.2,16,0,Active"
    "ND-003,DC-002,2024-10-01T12:00:00,45.0,0.01,1.5,16,0,Active"
    "ND-004,DC-002,2024-10-01T08:00:00,60.0,0.03,5.0,24,3,Active"
    "ND-004,DC-002,2024-10-01T12:00:00,75.0,0.12,8.5,24,12,Degraded"
    "ND-005,DC-003,2024-10-01T08:00:00,25.0,0.00,0.8,12,0,Active"
    "ND-005,DC-003,2024-10-01T12:00:00,35.0,0.01,1.0,12,0,Active"
    "ND-006,DC-003,2024-10-01T08:00:00,40.0,0.02,3.5,24,1,Active"
    "ND-006,DC-003,2024-10-01T12:00:00,65.0,0.05,5.0,24,4,Active"
    "ND-007,DC-004,2024-10-01T08:00:00,50.0,0.01,2.0,16,0,Active"
    "ND-007,DC-004,2024-10-01T12:00:00,58.0,0.02,2.5,16,1,Active"
    "ND-008,DC-004,2024-10-01T08:00:00,20.0,0.00,1.0,8,0,Active"
    "ND-009,DC-005,2024-10-01T08:00:00,35.0,0.01,15.0,8,0,Active"
    "ND-009,DC-005,2024-10-01T12:00:00,42.0,0.03,18.0,8,2,Active"
    "ND-010,DC-005,2024-10-01T08:00:00,28.0,0.00,12.0,4,0,Active"
    "ND-010,DC-005,2024-10-01T12:00:00,32.0,0.01,14.0,4,0,Active"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table NetworkMetric with (format='csv') <|`n$($netData -join "`n")" -Description "Ingesting 20 NetworkMetric rows..." | Out-Null; Write-Host "  [OK] NetworkMetric (20 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] NetworkMetric: $_" -ForegroundColor Yellow }

# Ã¢â€â‚¬Ã¢â€â‚¬ INGEST IncidentMetric Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
Write-Host "`n[Step 6] Ingesting IncidentMetric sample data..." -ForegroundColor Cyan

$incData = @(
    "INC-001,SRV-005,Critical,Performance,2024-10-01T08:30:00,2024-10-01T12:30:00,4.0,250,Disk I/O saturation from runaway query,Resolved"
    "INC-002,SRV-003,High,Performance,2024-10-01T09:15:00,2024-10-01T11:00:00,1.75,50,Memory leak in application APP-003,Resolved"
    "INC-003,SRV-007,Critical,Hardware,2024-10-01T10:00:00,2024-10-02T04:00:00,18.0,15,GPU failure requiring replacement,Resolved"
    "INC-004,SRV-004,Critical,Service,2024-10-01T16:30:00,2024-10-01T18:00:00,1.5,500,Database service crash due to connection pool exhaustion,Resolved"
    "INC-005,SRV-001,Medium,Performance,2024-10-01T11:30:00,2024-10-01T12:00:00,0.5,20,Disk IOPS spike from batch indexing,Resolved"
    "INC-006,SRV-009,High,Performance,2024-10-01T12:45:00,2024-10-01T14:30:00,1.75,100,CPU saturated by ML training job,Resolved"
    "INC-007,SRV-002,Medium,Network,2024-10-01T14:00:00,2024-10-01T15:30:00,1.5,30,NIC flapping causing intermittent latency,Resolved"
    "INC-008,SRV-011,High,Storage,2024-10-01T15:15:00,2024-10-02T08:00:00,16.75,200,Disk space critical - emergency cleanup required,Resolved"
    "INC-009,SRV-005,Critical,Performance,2024-10-02T10:15:00,,0,250,Repeat CPU overload - investigation ongoing,Open"
    "INC-010,SRV-010,High,Network,2024-10-02T14:30:00,,0,80,Cross-DC replication lag impacting DR,Open"
    "INC-011,SRV-008,Medium,Performance,2024-10-02T09:00:00,2024-10-02T11:00:00,2.0,15,AI training memory pressure,Resolved"
    "INC-012,SRV-006,Medium,Security,2024-10-02T13:00:00,2024-10-02T13:30:00,0.5,0,Failed login attempts detected - account locked,Resolved"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table IncidentMetric with (format='csv') <|`n$($incData -join "`n")" -Description "Ingesting 12 IncidentMetric rows..." | Out-Null; Write-Host "  [OK] IncidentMetric (12 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] IncidentMetric: $_" -ForegroundColor Yellow }

Write-Host "`n=== IT Asset KQL Tables Complete ===" -ForegroundColor Cyan
