<#
.SYNOPSIS
    Enrich all domain CSVs with additional sample data.
.DESCRIPTION
    Generates and appends realistic sample data to SensorTelemetry,
    Fact tables, and Dimension tables across all 5 ontology domains.
    Target: 300+ total rows per domain.
#>
param(
    [string]$BasePath = (Join-Path $PSScriptRoot "ontologies")
)

$ErrorActionPreference = "Stop"
Write-Host "=== Enriching Ontology Sample Data ===" -ForegroundColor Cyan

function Get-NextId {
    param([string]$Path, [string]$Prefix, [int]$PadWidth = 3)
    $rows = Import-Csv $Path
    $maxNum = 0
    foreach ($r in $rows) {
        $val = $r.PSObject.Properties.Value | Select-Object -First 1
        if ($val -match "$Prefix(\d+)") { $n = [int]$Matches[1]; if ($n -gt $maxNum) { $maxNum = $n } }
    }
    return $maxNum + 1
}

# ══════════════════════════════════════════════════════════════════════════════
# IT ASSET
# ══════════════════════════════════════════════════════════════════════════════
Write-Host "`n--- IT Asset ---" -ForegroundColor Yellow
$itData = Join-Path $BasePath "ITAsset\data"

# SensorTelemetry: ReadingId,ServerId,Timestamp,CPUPercent,MemoryPercent,DiskIOPS,NetworkMbps,QualityFlag,IsAnomaly
$itTelemetry = Join-Path $itData "SensorTelemetry.csv"
$servers = @("SRV-001","SRV-002","SRV-003","SRV-004","SRV-005","SRV-006","SRV-007","SRV-008","SRV-009","SRV-010","SRV-011","SRV-012","SRV-013","SRV-014","SRV-015")
$nextId = Get-NextId -Path $itTelemetry -Prefix "TR-"
$newRows = @()
$dates = @("2024-10-01","2024-10-02","2024-10-03","2024-10-04","2024-10-05","2024-10-06","2024-10-07")
$hours = @("09:00:00","09:15:00","09:30:00","09:45:00","10:00:00","10:15:00","10:30:00","12:00:00","14:00:00","16:00:00","18:00:00","20:00:00")
foreach ($dt in $dates) {
    foreach ($srv in ($servers | Get-Random -Count 8)) {
        foreach ($hr in ($hours | Get-Random -Count 4)) {
            $cpu = [Math]::Round((Get-Random -Minimum 15 -Maximum 95) + (Get-Random -Minimum 0 -Maximum 100) / 100, 1)
            $mem = [Math]::Round((Get-Random -Minimum 30 -Maximum 92) + (Get-Random -Minimum 0 -Maximum 100) / 100, 1)
            $disk = Get-Random -Minimum 200 -Maximum 5500
            $net = Get-Random -Minimum 50 -Maximum 2500
            $anomaly = if ($cpu -gt 90 -or $mem -gt 88 -or $disk -gt 5000) { "true" } else { "false" }
            $quality = if ($anomaly -eq "true") { "Degraded" } else { "Good" }
            $id = "TR-" + $nextId.ToString().PadLeft(3,'0')
            $newRows += [PSCustomObject]@{ ReadingId=$id; ServerId=$srv; Timestamp="${dt}T${hr}"; CPUPercent=$cpu; MemoryPercent=$mem; DiskIOPS=$disk; NetworkMbps=$net; QualityFlag=$quality; IsAnomaly=$anomaly }
            $nextId++
        }
    }
}
$newRows | Export-Csv -Path $itTelemetry -Append -NoTypeInformation -Force
Write-Host "  SensorTelemetry: +$($newRows.Count) rows" -ForegroundColor Green

# FactAlert: AlertId,ServerId,AlertType,Severity,Timestamp,MetricName,MetricValue,ThresholdValue,Message,IsAcknowledged
$itAlert = Join-Path $itData "FactAlert.csv"
$nextAlertId = Get-NextId -Path $itAlert -Prefix "ALT-"
$alertTypes = @("HighCPU","HighMemory","DiskFull","NetworkSaturation","ServiceDown","HighIOPS","LatencySpike","CertExpiring")
$severities = @("Critical","High","Medium","Low")
$newAlerts = @()
foreach ($dt in @("2024-10-02","2024-10-03","2024-10-04","2024-10-05","2024-10-06","2024-10-07","2024-10-08")) {
    $count = Get-Random -Minimum 2 -Maximum 6
    for ($i = 0; $i -lt $count; $i++) {
        $srv = $servers | Get-Random
        $atype = $alertTypes | Get-Random
        $sev = $severities | Get-Random
        $hr = Get-Random -Minimum 0 -Maximum 23
        $ack = if ((Get-Random -Minimum 0 -Maximum 10) -gt 3) { "true" } else { "false" }
        $id = "ALT-" + $nextAlertId.ToString().PadLeft(3,'0')
        $val = Get-Random -Minimum 80 -Maximum 100
        $thresh = Get-Random -Minimum 70 -Maximum 90
        $newAlerts += [PSCustomObject]@{ AlertId=$id; ServerId=$srv; AlertType=$atype; Severity=$sev; Timestamp="${dt}T$($hr.ToString().PadLeft(2,'0')):00:00"; MetricName=$atype; MetricValue=$val; ThresholdValue=$thresh; Message="$atype on $srv"; IsAcknowledged=$ack }
        $nextAlertId++
    }
}
$newAlerts | Export-Csv -Path $itAlert -Append -NoTypeInformation -Force
Write-Host "  FactAlert: +$($newAlerts.Count) rows" -ForegroundColor Green

# FactIncident
$itIncident = Join-Path $itData "FactIncident.csv"
$nextIncId = Get-NextId -Path $itIncident -Prefix "INC-"
$incTypes = @("Performance","Availability","Security","Configuration","Network","Hardware")
$rootCauses = @("Memory leak","Disk I/O saturation","Config change","Network partition","Software bug","Hardware fault","Certificate expired","Patch regression")
$users = @("USR-001","USR-002","USR-003","USR-004","USR-005","USR-006","USR-007","USR-008","USR-009","USR-010")
$newInc = @()
foreach ($dt in @("2024-02-01","2024-02-15","2024-03-01","2024-03-10","2024-03-20","2024-04-01","2024-04-15","2024-05-01")) {
    $srv = $servers | Get-Random
    $itype = $incTypes | Get-Random
    $sev = $severities | Get-Random
    $usr = $users | Get-Random
    $dur = [Math]::Round((Get-Random -Minimum 5 -Maximum 240) / 10, 1)
    $root = $rootCauses | Get-Random
    $status = if ((Get-Random -Minimum 0 -Maximum 10) -gt 2) { "Resolved" } else { "Open" }
    $id = "INC-" + $nextIncId.ToString().PadLeft(3,'0')
    $newInc += [PSCustomObject]@{ IncidentId=$id; ServerId=$srv; IncidentType=$itype; Severity=$sev; ReportedByUserId=$usr; CreatedDate=$dt; ResolvedDate=if($status -eq "Resolved"){$dt}else{""}; DurationHours=$dur; RootCause=$root; Description="$itype issue on $srv - $root"; Status=$status }
    $nextIncId++
}
$newInc | Export-Csv -Path $itIncident -Append -NoTypeInformation -Force
Write-Host "  FactIncident: +$($newInc.Count) rows" -ForegroundColor Green

# ══════════════════════════════════════════════════════════════════════════════
# MANUFACTURING PLANT
# ══════════════════════════════════════════════════════════════════════════════
Write-Host "`n--- Manufacturing Plant ---" -ForegroundColor Yellow
$mfData = Join-Path $BasePath "ManufacturingPlant\data"

# SensorTelemetry: ReadingId,SensorId,Timestamp,ReadingValue,QualityFlag,IsAnomaly
$mfTelemetry = Join-Path $mfData "SensorTelemetry.csv"
$mfSensors = @("SNS-001","SNS-002","SNS-003","SNS-004","SNS-005","SNS-006","SNS-007","SNS-008","SNS-009","SNS-010","SNS-011","SNS-012","SNS-013","SNS-014","SNS-015","SNS-016","SNS-017","SNS-018","SNS-019","SNS-020")
$nextId = Get-NextId -Path $mfTelemetry -Prefix "TR-"
$newRows = @()
foreach ($dt in @("2024-10-01","2024-10-02","2024-10-03","2024-10-04","2024-10-05","2024-10-06","2024-10-07")) {
    foreach ($sns in ($mfSensors | Get-Random -Count 10)) {
        foreach ($hr in ($hours | Get-Random -Count 3)) {
            $val = [Math]::Round((Get-Random -Minimum 100 -Maximum 2000) + (Get-Random -Minimum 0 -Maximum 100) / 100, 1)
            $anomaly = if ((Get-Random -Minimum 0 -Maximum 100) -gt 92) { "true" } else { "false" }
            $quality = if ($anomaly -eq "true") { "Suspect" } else { "Good" }
            $id = "TR-" + $nextId.ToString().PadLeft(3,'0')
            $newRows += [PSCustomObject]@{ ReadingId=$id; SensorId=$sns; Timestamp="${dt}T${hr}"; ReadingValue=$val; QualityFlag=$quality; IsAnomaly=$anomaly }
            $nextId++
        }
    }
}
$newRows | Export-Csv -Path $mfTelemetry -Append -NoTypeInformation -Force
Write-Host "  SensorTelemetry: +$($newRows.Count) rows" -ForegroundColor Green

# FactProductionBatch
$mfProd = Join-Path $mfData "FactProductionBatch.csv"
$existing = Import-Csv $mfProd
$header = ($existing | Get-Member -MemberType NoteProperty).Name
$nextBatchId = Get-NextId -Path $mfProd -Prefix "PB-"
$products = @("PRD-001","PRD-002","PRD-003","PRD-004","PRD-005","PRD-006","PRD-007","PRD-008")
$lines = @("PL-001","PL-002","PL-003","PL-004","PL-005","PL-006","PL-007","PL-008","PL-009","PL-010")
$operators = @("OP-001","OP-002","OP-003","OP-004","OP-005","OP-006","OP-007","OP-008","OP-009","OP-010","OP-011","OP-012")
$statuses = @("Completed","Completed","Completed","InProgress","Completed","QualityHold")
$newBatches = @()
foreach ($dt in @("2024-10-08","2024-10-09","2024-10-10","2024-10-11","2024-10-12","2024-10-13","2024-10-14","2024-10-15")) {
    $count = Get-Random -Minimum 2 -Maximum 5
    for ($i = 0; $i -lt $count; $i++) {
        $id = "PB-" + $nextBatchId.ToString().PadLeft(3,'0')
        $prd = $products | Get-Random
        $ln = $lines | Get-Random
        $op = $operators | Get-Random
        $qty = Get-Random -Minimum 50 -Maximum 500
        $defectRate = [Math]::Round((Get-Random -Minimum 0 -Maximum 80) / 10, 2)
        $energy = [Math]::Round((Get-Random -Minimum 100 -Maximum 800) + (Get-Random -Minimum 0 -Maximum 100) / 100, 1)
        $cycle = [Math]::Round((Get-Random -Minimum 30 -Maximum 180) + (Get-Random -Minimum 0 -Maximum 100) / 100, 1)
        $oee = [Math]::Round((Get-Random -Minimum 60 -Maximum 98) + (Get-Random -Minimum 0 -Maximum 100) / 100, 1)
        $st = $statuses | Get-Random
        $newBatches += [PSCustomObject]@{ BatchId=$id; ProductId=$prd; LineId=$ln; OperatorId=$op; StartTime="${dt}T06:00:00"; EndTime="${dt}T14:00:00"; QuantityProduced=$qty; DefectRate=$defectRate; EnergyUsedKWh=$energy; CycleTimeMinutes=$cycle; OEEPercent=$oee; Status=$st }
        $nextBatchId++
    }
}
$newBatches | Export-Csv -Path $mfProd -Append -NoTypeInformation -Force
Write-Host "  FactProductionBatch: +$($newBatches.Count) rows" -ForegroundColor Green

# FactQualityCheck
$mfQual = Join-Path $mfData "FactQualityCheck.csv"
$nextQcId = Get-NextId -Path $mfQual -Prefix "QC-"
$testTypes = @("Dimensional","Visual","Tensile","Hardness","Surface","Chemical","Functional")
$results = @("Pass","Pass","Pass","Pass","Fail","Marginal")
$newQc = @()
foreach ($dt in @("2024-10-08","2024-10-09","2024-10-10","2024-10-11","2024-10-12","2024-10-13","2024-10-14")) {
    $count = Get-Random -Minimum 3 -Maximum 6
    for ($i = 0; $i -lt $count; $i++) {
        $id = "QC-" + $nextQcId.ToString().PadLeft(3,'0')
        $batch = "PB-" + (Get-Random -Minimum 1 -Maximum $nextBatchId).ToString().PadLeft(3,'0')
        $test = $testTypes | Get-Random
        $result = $results | Get-Random
        $measured = [Math]::Round((Get-Random -Minimum 80 -Maximum 120) + (Get-Random -Minimum 0 -Maximum 100) / 100, 2)
        $specMin = [Math]::Round($measured - (Get-Random -Minimum 5 -Maximum 20), 2)
        $specMax = [Math]::Round($measured + (Get-Random -Minimum 5 -Maximum 20), 2)
        $op = $operators | Get-Random
        $newQc += [PSCustomObject]@{ CheckId=$id; BatchId=$batch; TestType=$test; Result=$result; MeasuredValue=$measured; SpecMin=$specMin; SpecMax=$specMax; InspectorId=$op; Timestamp="${dt}T10:00:00" }
        $nextQcId++
    }
}
$newQc | Export-Csv -Path $mfQual -Append -NoTypeInformation -Force
Write-Host "  FactQualityCheck: +$($newQc.Count) rows" -ForegroundColor Green

# FactAlert
$mfAlert = Join-Path $mfData "FactAlert.csv"
$nextAlertId = Get-NextId -Path $mfAlert -Prefix "ALT-"
$mfAlertTypes = @("HighTemperature","ExcessiveVibration","PressureDrop","SpeedDeviation","OilLevelLow","PowerSurge","EmergencyStop")
$mfSeverities = @("Critical","High","Medium","Low")
$machines = @("MC-001","MC-002","MC-003","MC-004","MC-005","MC-006","MC-007","MC-008","MC-009","MC-010","MC-011","MC-012","MC-013","MC-014","MC-015","MC-016","MC-017","MC-018","MC-019","MC-020")
$newMfAlerts = @()
foreach ($dt in @("2024-10-02","2024-10-03","2024-10-04","2024-10-05","2024-10-06","2024-10-07","2024-10-08","2024-10-09","2024-10-10")) {
    $count = Get-Random -Minimum 1 -Maximum 4
    for ($i = 0; $i -lt $count; $i++) {
        $id = "ALT-" + $nextAlertId.ToString().PadLeft(3,'0')
        $mc = $machines | Get-Random
        $at = $mfAlertTypes | Get-Random
        $sev = $mfSeverities | Get-Random
        $hr = Get-Random -Minimum 6 -Maximum 22
        $ack = if ((Get-Random -Minimum 0 -Maximum 10) -gt 3) { "true" } else { "false" }
        $newMfAlerts += [PSCustomObject]@{ AlertId=$id; MachineId=$mc; SensorId=($mfSensors | Get-Random); AlertType=$at; Severity=$sev; Timestamp="${dt}T$($hr.ToString().PadLeft(2,'0')):00:00"; MetricValue=(Get-Random -Minimum 80 -Maximum 200); ThresholdValue=(Get-Random -Minimum 70 -Maximum 150); Message="$at detected on $mc"; IsAcknowledged=$ack }
        $nextAlertId++
    }
}
$newMfAlerts | Export-Csv -Path $mfAlert -Append -NoTypeInformation -Force
Write-Host "  FactAlert: +$($newMfAlerts.Count) rows" -ForegroundColor Green

# ══════════════════════════════════════════════════════════════════════════════
# SMART BUILDING
# ══════════════════════════════════════════════════════════════════════════════
Write-Host "`n--- Smart Building ---" -ForegroundColor Yellow
$sbData = Join-Path $BasePath "SmartBuilding\data"

# SensorTelemetry: ReadingId,SensorId,Timestamp,ReadingValue,QualityFlag,IsAnomaly
$sbTelemetry = Join-Path $sbData "SensorTelemetry.csv"
$sbSensors = @("SNS-001","SNS-002","SNS-003","SNS-004","SNS-005","SNS-006","SNS-007","SNS-008","SNS-009","SNS-010","SNS-011","SNS-012","SNS-013","SNS-014","SNS-015","SNS-016","SNS-017","SNS-018","SNS-019","SNS-020","SNS-021","SNS-022","SNS-023","SNS-024","SNS-025","SNS-026","SNS-027","SNS-028","SNS-029","SNS-030")
$nextId = Get-NextId -Path $sbTelemetry -Prefix "TR-"
$newRows = @()
foreach ($dt in @("2024-10-01","2024-10-02","2024-10-03","2024-10-04","2024-10-05","2024-10-06","2024-10-07")) {
    foreach ($sns in ($sbSensors | Get-Random -Count 12)) {
        foreach ($hr in ($hours | Get-Random -Count 3)) {
            $val = [Math]::Round((Get-Random -Minimum 18 -Maximum 85) + (Get-Random -Minimum 0 -Maximum 100) / 100, 1)
            $anomaly = if ((Get-Random -Minimum 0 -Maximum 100) -gt 93) { "true" } else { "false" }
            $quality = if ($anomaly -eq "true") { "Suspect" } else { "Good" }
            $id = "TR-" + $nextId.ToString().PadLeft(3,'0')
            $newRows += [PSCustomObject]@{ ReadingId=$id; SensorId=$sns; Timestamp="${dt}T${hr}"; ReadingValue=$val; QualityFlag=$quality; IsAnomaly=$anomaly }
            $nextId++
        }
    }
}
$newRows | Export-Csv -Path $sbTelemetry -Append -NoTypeInformation -Force
Write-Host "  SensorTelemetry: +$($newRows.Count) rows" -ForegroundColor Green

# FactAlert
$sbAlert = Join-Path $sbData "FactAlert.csv"
$nextAlertId = Get-NextId -Path $sbAlert -Prefix "ALT-"
$sbAlertTypes = @("HighTemperature","LowAirQuality","MotionAfterHours","WaterLeak","SmokeDetected","DoorForcedOpen","HVACFailure","ElevatorFault","PowerFluctuation")
$zones = @("ZN-001","ZN-002","ZN-003","ZN-004","ZN-005","ZN-006","ZN-007","ZN-008","ZN-009","ZN-010","ZN-011","ZN-012","ZN-013","ZN-014","ZN-015","ZN-016","ZN-017","ZN-018","ZN-019","ZN-020","ZN-021","ZN-022","ZN-023","ZN-024","ZN-025")
$newSbAlerts = @()
foreach ($dt in @("2024-10-02","2024-10-03","2024-10-04","2024-10-05","2024-10-06","2024-10-07","2024-10-08","2024-10-09","2024-10-10")) {
    $count = Get-Random -Minimum 1 -Maximum 5
    for ($i = 0; $i -lt $count; $i++) {
        $id = "ALT-" + $nextAlertId.ToString().PadLeft(3,'0')
        $zn = $zones | Get-Random
        $sn = $sbSensors | Get-Random
        $at = $sbAlertTypes | Get-Random
        $sev = $severities | Get-Random
        $hr = Get-Random -Minimum 0 -Maximum 23
        $ack = if ((Get-Random -Minimum 0 -Maximum 10) -gt 3) { "true" } else { "false" }
        $newSbAlerts += [PSCustomObject]@{ AlertId=$id; ZoneId=$zn; SensorId=$sn; AlertType=$at; Severity=$sev; Timestamp="${dt}T$($hr.ToString().PadLeft(2,'0')):00:00"; MetricValue=[Math]::Round((Get-Random -Minimum 50 -Maximum 150) + 0.5, 1); ThresholdValue=[Math]::Round((Get-Random -Minimum 40 -Maximum 100) + 0.5, 1); Message="$at in zone $zn"; IsAcknowledged=$ack }
        $nextAlertId++
    }
}
$newSbAlerts | Export-Csv -Path $sbAlert -Append -NoTypeInformation -Force
Write-Host "  FactAlert: +$($newSbAlerts.Count) rows" -ForegroundColor Green

# FactMaintenanceTicket
$sbMaint = Join-Path $sbData "FactMaintenanceTicket.csv"
$nextMaintId = Get-NextId -Path $sbMaint -Prefix "MT-"
$ticketTypes = @("HVAC Repair","Lighting Replacement","Elevator Service","Plumbing","Electrical","Fire System","Security System","Cleaning")
$ticketPriority = @("Emergency","High","Medium","Low")
$ticketStatus = @("Completed","Completed","Completed","InProgress","Scheduled")
$newMaint = @()
foreach ($dt in @("2024-10-05","2024-10-08","2024-10-11","2024-10-14","2024-10-17","2024-10-20","2024-10-23","2024-10-26","2024-10-29","2024-11-01")) {
    $id = "MT-" + $nextMaintId.ToString().PadLeft(3,'0')
    $zn = $zones | Get-Random
    $tt = $ticketTypes | Get-Random
    $pr = $ticketPriority | Get-Random
    $st = $ticketStatus | Get-Random
    $cost = [Math]::Round((Get-Random -Minimum 50 -Maximum 5000) + (Get-Random -Minimum 0 -Maximum 100) / 100, 2)
    $dur = [Math]::Round((Get-Random -Minimum 5 -Maximum 480) / 10, 1)
    $newMaint += [PSCustomObject]@{ TicketId=$id; ZoneId=$zn; TicketType=$tt; Priority=$pr; CreatedDate=$dt; CompletedDate=if($st -eq "Completed"){$dt}else{""}; DurationHours=$dur; CostUSD=$cost; Description="$tt in zone $zn"; Status=$st }
    $nextMaintId++
}
$newMaint | Export-Csv -Path $sbMaint -Append -NoTypeInformation -Force
Write-Host "  FactMaintenanceTicket: +$($newMaint.Count) rows" -ForegroundColor Green

# ══════════════════════════════════════════════════════════════════════════════
# WIND TURBINE
# ══════════════════════════════════════════════════════════════════════════════
Write-Host "`n--- Wind Turbine ---" -ForegroundColor Yellow
$wtData = Join-Path $BasePath "WindTurbine\data"

# SensorTelemetry: Timestamp,TurbineId,SensorId,SensorType,Value,Unit,Quality
$wtTelemetry = Join-Path $wtData "SensorTelemetry.csv"
$turbines = @("WT-001","WT-002","WT-003","WT-004","WT-005","WT-006","WT-007","WT-008","WT-009","WT-010","WT-011","WT-012","WT-013","WT-014","WT-015")
$wtSensors = @("SN-001","SN-002","SN-003","SN-004","SN-005","SN-006","SN-007","SN-008","SN-009","SN-010","SN-011","SN-012","SN-013","SN-014","SN-015","SN-016","SN-017","SN-018","SN-019","SN-020")
$sensorTypes = @(
    @{ Type = "Anemometer"; Unit = "m/s"; Min = 2; Max = 28 },
    @{ Type = "Accelerometer"; Unit = "mm/s"; Min = 0.5; Max = 10 },
    @{ Type = "Thermometer"; Unit = "C"; Min = 15; Max = 110 },
    @{ Type = "Tachometer"; Unit = "RPM"; Min = 5; Max = 18 },
    @{ Type = "StrainGauge"; Unit = "MPa"; Min = 10; Max = 200 },
    @{ Type = "CurrentSensor"; Unit = "A"; Min = 50; Max = 800 }
)
$qualities = @("Good","Good","Good","Good","Good","Good","Good","Uncertain","Bad")
$newRows = @()
foreach ($dt in @("2024-11-16","2024-11-17","2024-11-18","2024-11-19","2024-11-20")) {
    foreach ($turb in ($turbines | Get-Random -Count 8)) {
        foreach ($hrOff in @(0, 4, 8, 12, 16, 20)) {
            $st = $sensorTypes | Get-Random
            $sns = $wtSensors | Get-Random
            $val = [Math]::Round((Get-Random -Minimum ($st.Min * 10) -Maximum ($st.Max * 10)) / 10, 1)
            $quality = $qualities | Get-Random
            $ts = "${dt}T$($hrOff.ToString().PadLeft(2,'0')):00:00Z"
            $newRows += [PSCustomObject]@{ Timestamp=$ts; TurbineId=$turb; SensorId=$sns; SensorType=$st.Type; Value=$val; Unit=$st.Unit; Quality=$quality }
        }
    }
}
$newRows | Export-Csv -Path $wtTelemetry -Append -NoTypeInformation -Force
Write-Host "  SensorTelemetry: +$($newRows.Count) rows" -ForegroundColor Green

# FactPowerOutput
$wtPower = Join-Path $wtData "FactPowerOutput.csv"
$nextPoId = Get-NextId -Path $wtPower -Prefix "PO-"
$newPower = @()
foreach ($dt in @("2024-11-16","2024-11-17","2024-11-18","2024-11-19","2024-11-20")) {
    foreach ($turb in ($turbines | Get-Random -Count 10)) {
        foreach ($hr in @(0, 6, 12, 18)) {
            $windSpeed = [Math]::Round((Get-Random -Minimum 30 -Maximum 220) / 10, 1)
            $ratedKW = 8000
            $power = if ($windSpeed -lt 3) { 0 } elseif ($windSpeed -gt 25) { 0 } elseif ($windSpeed -gt 12) { [Math]::Round($ratedKW * (0.85 + (Get-Random -Minimum 0 -Maximum 15) / 100), 0) } else { [Math]::Round($ratedKW * ($windSpeed / 12) * (0.7 + (Get-Random -Minimum 0 -Maximum 25) / 100), 0) }
            $cf = [Math]::Round($power / $ratedKW, 2)
            $rpm = [Math]::Round(8 + $windSpeed * 0.4 + (Get-Random -Minimum -10 -Maximum 10) / 10, 1)
            $pitch = [Math]::Round((Get-Random -Minimum 0 -Maximum 150) / 10, 1)
            $yaw = Get-Random -Minimum 0 -Maximum 360
            $grid = [Math]::Round(49.9 + (Get-Random -Minimum 0 -Maximum 20) / 100, 2)
            $id = "PO-" + $nextPoId.ToString().PadLeft(3,'0')
            $newPower += [PSCustomObject]@{ OutputId=$id; TurbineId=$turb; Date=$dt; Hour=$hr; WindSpeedMs=$windSpeed; PowerOutputKW=$power; CapacityFactor=$cf; RotorRPM=$rpm; PitchAngleDeg=$pitch; YawAngleDeg=$yaw; GridFrequencyHz=$grid }
            $nextPoId++
        }
    }
}
$newPower | Export-Csv -Path $wtPower -Append -NoTypeInformation -Force
Write-Host "  FactPowerOutput: +$($newPower.Count) rows" -ForegroundColor Green

# FactAlert
$wtAlert = Join-Path $wtData "FactAlert.csv"
$nextAlertId = Get-NextId -Path $wtAlert -Prefix "WA-"
$wtAlertTypes = @("HighVibration","HighTemperature","HighWindSpeed","LowPowerOutput","PitchAngle","YawError","BladeIcing","GridFrequency","CommunicationLoss","OilPressure")
$components = @("MainBearing","Gearbox","Generator","Blade","Nacelle","Tower","Transformer","SCADA","BladeHub","YawDrive")
$newWtAlerts = @()
foreach ($dt in @("2024-11-17","2024-11-18","2024-11-19","2024-11-20","2024-11-21")) {
    $count = Get-Random -Minimum 2 -Maximum 5
    for ($i = 0; $i -lt $count; $i++) {
        $id = "WA-" + $nextAlertId.ToString().PadLeft(3,'0')
        $turb = $turbines | Get-Random
        $farm = "WF-" + (Get-Random -Minimum 1 -Maximum 6).ToString().PadLeft(3,'0')
        $at = $wtAlertTypes | Get-Random
        $sev = $severities | Get-Random
        $comp = $components | Get-Random
        $hr = Get-Random -Minimum 0 -Maximum 23
        $ack = if ((Get-Random -Minimum 0 -Maximum 10) -gt 4) { "true" } else { "false" }
        $val = [Math]::Round((Get-Random -Minimum 50 -Maximum 250) / 10, 1)
        $thresh = [Math]::Round((Get-Random -Minimum 40 -Maximum 200) / 10, 1)
        $newWtAlerts += [PSCustomObject]@{ AlertId=$id; TurbineId=$turb; SensorId=($wtSensors | Get-Random); AlertType=$at; Severity=$sev; Timestamp="${dt}T$($hr.ToString().PadLeft(2,'0')):00:00"; MetricValue=$val; ThresholdValue=$thresh; Component=$comp; Message="$at on $comp of $turb"; IsAcknowledged=$ack }
        $nextAlertId++
    }
}
$newWtAlerts | Export-Csv -Path $wtAlert -Append -NoTypeInformation -Force
Write-Host "  FactAlert: +$($newWtAlerts.Count) rows" -ForegroundColor Green

# FactMaintenanceEvent
$wtMaint = Join-Path $wtData "FactMaintenanceEvent.csv"
$nextEvtId = Get-NextId -Path $wtMaint -Prefix "ME-"
$eventTypes = @("Preventive","Corrective","Inspection","Emergency","Overhaul")
$techs = @("TN-001","TN-002","TN-003","TN-004","TN-005","TN-006","TN-007","TN-008","TN-009","TN-010","TN-011","TN-012")
$eventStatuses = @("Completed","Completed","Completed","InProgress","Scheduled")
$newEvents = @()
foreach ($dt in @("2024-11-16","2024-11-17","2024-11-18","2024-11-19","2024-11-20","2024-11-21","2024-11-22","2024-11-23")) {
    $count = Get-Random -Minimum 1 -Maximum 3
    for ($i = 0; $i -lt $count; $i++) {
        $id = "ME-" + $nextEvtId.ToString().PadLeft(3,'0')
        $turb = $turbines | Get-Random
        $tech = $techs | Get-Random
        $et = $eventTypes | Get-Random
        $comp = $components | Get-Random
        $dur = [Math]::Round((Get-Random -Minimum 10 -Maximum 480) / 10, 1)
        $cost = [Math]::Round((Get-Random -Minimum 200 -Maximum 25000) + (Get-Random -Minimum 0 -Maximum 100) / 100, 2)
        $st = $eventStatuses | Get-Random
        $newEvents += [PSCustomObject]@{ EventId=$id; TurbineId=$turb; TechnicianId=$tech; EventType=$et; Component=$comp; StartDate=$dt; DurationHours=$dur; CostUSD=$cost; Status=$st }
        $nextEvtId++
    }
}
$newEvents | Export-Csv -Path $wtMaint -Append -NoTypeInformation -Force
Write-Host "  FactMaintenanceEvent: +$($newEvents.Count) rows" -ForegroundColor Green

# ══════════════════════════════════════════════════════════════════════════════
# OIL & GAS REFINERY (already has 410 rows, add modest enrichment to Fact tables)
# ══════════════════════════════════════════════════════════════════════════════
Write-Host "`n--- Oil & Gas Refinery ---" -ForegroundColor Yellow
$ogData = Join-Path $BasePath "OilGasRefinery\data"

# FactSafetyAlarm
$ogAlarm = Join-Path $ogData "FactSafetyAlarm.csv"
if (Test-Path $ogAlarm) {
    $nextAlarmId = Get-NextId -Path $ogAlarm -Prefix "SA-"
    $ogEquip = @("EQ-001","EQ-002","EQ-003","EQ-004","EQ-005","EQ-006","EQ-007","EQ-008","EQ-009","EQ-010","EQ-011","EQ-012","EQ-013","EQ-014","EQ-015")
    $ogSensors = @("S-001","S-002","S-003","S-004","S-005","S-006","S-007","S-008","S-009","S-010","S-011","S-012","S-013","S-014","S-015","S-016","S-017","S-018","S-019","S-020")
    $ogAlarmTypes = @("HighPressure","LowPressure","HighTemperature","LowFlow","GasLeak","Overspeed","HighLevel","LowLevel","Vibration","EmergencyShutdown")
    $newAlarms = @()
    foreach ($dt in @("2024-10-08","2024-10-09","2024-10-10","2024-10-11","2024-10-12","2024-10-13","2024-10-14")) {
        $count = Get-Random -Minimum 2 -Maximum 5
        for ($i = 0; $i -lt $count; $i++) {
            $id = "SA-" + $nextAlarmId.ToString().PadLeft(3,'0')
            $eq = $ogEquip | Get-Random
            $sn = $ogSensors | Get-Random
            $at = $ogAlarmTypes | Get-Random
            $sev = $severities | Get-Random
            $hr = Get-Random -Minimum 0 -Maximum 23
            $ack = if ((Get-Random -Minimum 0 -Maximum 10) -gt 3) { "true" } else { "false" }
            $newAlarms += [PSCustomObject]@{ AlarmId=$id; EquipmentId=$eq; SensorId=$sn; AlarmType=$at; Severity=$sev; Timestamp="${dt}T$($hr.ToString().PadLeft(2,'0')):00:00"; Value=[Math]::Round((Get-Random -Minimum 50 -Maximum 500) + 0.5, 1); Threshold=[Math]::Round((Get-Random -Minimum 40 -Maximum 400) + 0.5, 1); Message="$at on $eq"; IsAcknowledged=$ack }
            $nextAlarmId++
        }
    }
    $newAlarms | Export-Csv -Path $ogAlarm -Append -NoTypeInformation -Force
    Write-Host "  FactSafetyAlarm: +$($newAlarms.Count) rows" -ForegroundColor Green
}

# FactProduction
$ogProd = Join-Path $ogData "FactProduction.csv"
if (Test-Path $ogProd) {
    $existing = Import-Csv $ogProd
    $nextProdId = Get-NextId -Path $ogProd -Prefix "FP-"
    $refineries = @("R-001","R-002","R-003","R-004","R-005","R-006","R-007","R-008")
    $processUnits = @("PU-001","PU-002","PU-003","PU-004","PU-005","PU-006","PU-007","PU-008","PU-009","PU-010")
    $newProd = @()
    foreach ($dt in @("2024-10-08","2024-10-09","2024-10-10","2024-10-11","2024-10-12")) {
        foreach ($ref in ($refineries | Get-Random -Count 4)) {
            $pu = $processUnits | Get-Random
            $id = "FP-" + $nextProdId.ToString().PadLeft(3,'0')
            $input = Get-Random -Minimum 5000 -Maximum 50000
            $output = [Math]::Round($input * (0.7 + (Get-Random -Minimum 0 -Maximum 25) / 100), 0)
            $yield = [Math]::Round($output / $input * 100, 1)
            $energy = [Math]::Round((Get-Random -Minimum 100 -Maximum 2000) + (Get-Random -Minimum 0 -Maximum 100) / 100, 1)
            $newProd += [PSCustomObject]@{ ProductionId=$id; RefineryId=$ref; ProcessUnitId=$pu; ProductionDate=$dt; InputBarrels=$input; OutputBarrels=$output; YieldPercent=$yield; EnergyMWh=$energy; Status="Completed" }
            $nextProdId++
        }
    }
    $newProd | Export-Csv -Path $ogProd -Append -NoTypeInformation -Force
    Write-Host "  FactProduction: +$($newProd.Count) rows" -ForegroundColor Green
}

Write-Host "`n=== Data Enrichment Complete ===" -ForegroundColor Cyan
Write-Host "Run this script again to add more data. Each run appends new rows." -ForegroundColor Gray
