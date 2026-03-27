<#
.SYNOPSIS
    Simulates real-time telemetry events and sends them to the domain's Eventstream.
.DESCRIPTION
    Generates realistic sensor readings for any supported domain and sends JSON
    events to the Eventstream via its Event Hub-compatible endpoint.

    Each domain produces domain-specific telemetry:
      - OilGasRefinery:     SensorReading (Temperature, Pressure, FlowRate, etc.)
      - SmartBuilding:      SensorReading (Temperature, Humidity, CO2, Occupancy)
      - ManufacturingPlant: SensorReading (Vibration, Temperature, Pressure, Speed)
      - ITAsset:            ServerMetric  (CPU%, Memory%, DiskIOPS, NetworkMbps)
      - WindTurbine:        TurbineReading (WindSpeed, PowerOutput, RotorRPM, PitchAngle)
      - Healthcare:         PatientVitals (HeartRate, BP, Temperature, SpO2, RespRate)

    PREREQUISITES:
      - pip install azure-eventhub   (or use --RestOnly for pure REST/HTTP fallback)
      - Eventstream connection string from Fabric UI

.PARAMETER ConnectionString
    The Event Hub-compatible connection string from the Eventstream Custom App source.
    Get it from: Fabric > Eventstream > Custom App source node > Keys.

.PARAMETER OntologyType
    Domain key for event shape.

.PARAMETER EventCount
    Total number of events to send (default: 100).

.PARAMETER IntervalMs
    Milliseconds between events (default: 1000 = 1 per second).

.PARAMETER BatchSize
    Events per batch send (default: 10).

.EXAMPLE
    .\Send-TelemetrySimulator.ps1 -ConnectionString "Endpoint=sb://..." -OntologyType OilGasRefinery -EventCount 500
#>
param(
    [Parameter(Mandatory=$true)]
    [string]$ConnectionString,

    [ValidateSet("OilGasRefinery","SmartBuilding","ManufacturingPlant","ITAsset","WindTurbine","Healthcare")]
    [string]$OntologyType = "OilGasRefinery",

    [int]$EventCount = 100,
    [int]$IntervalMs = 1000,
    [int]$BatchSize = 10
)

# ── Domain-specific event generators ────────────────────────────────────────

function Get-RandomDouble { param([double]$Min, [double]$Max); return [math]::Round($Min + (Get-Random -Maximum 10000) / 10000.0 * ($Max - $Min), 2) }

function New-OilGasEvent {
    $sensorTypes = @("Temperature","Pressure","FlowRate","Level","Vibration","RPM")
    $units = @{ Temperature="Fahrenheit"; Pressure="PSI"; FlowRate="BPH"; Level="Percent"; Vibration="mm/s"; RPM="RPM" }
    $ranges = @{ Temperature=@(150,650); Pressure=@(10,500); FlowRate=@(100,2000); Level=@(5,98); Vibration=@(0.1,15); RPM=@(500,5000) }
    $type = $sensorTypes | Get-Random
    $r = $ranges[$type]
    return @{
        SensorId = "SEN-$(Get-Random -Minimum 1 -Maximum 50)"
        EquipmentId = "EQ-$(Get-Random -Minimum 1 -Maximum 30)"
        RefineryId = "REF-$(Get-Random -Minimum 1 -Maximum 8)"
        Timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        ReadingValue = Get-RandomDouble -Min $r[0] -Max $r[1]
        MeasurementUnit = $units[$type]
        SensorType = $type
        QualityFlag = @("Good","Good","Good","Suspect","Bad") | Get-Random
        IsAnomaly = ((Get-Random -Maximum 100) -lt 5)
    }
}

function New-SmartBuildingEvent {
    $sensorTypes = @("Temperature","Humidity","CO2","LightLevel","Occupancy","AirQuality")
    $units = @{ Temperature="Fahrenheit"; Humidity="Percent"; CO2="PPM"; LightLevel="Lux"; Occupancy="Count"; AirQuality="AQI" }
    $ranges = @{ Temperature=@(62,82); Humidity=@(25,70); CO2=@(350,1200); LightLevel=@(50,800); Occupancy=@(0,50); AirQuality=@(10,200) }
    $type = $sensorTypes | Get-Random
    $r = $ranges[$type]
    return @{
        SensorId = "SEN-$(Get-Random -Minimum 1 -Maximum 80)"
        ZoneId = "ZONE-$(Get-Random -Minimum 1 -Maximum 40)"
        BuildingId = "BLD-$(Get-Random -Minimum 1 -Maximum 8)"
        FloorId = "FLR-$(Get-Random -Minimum 1 -Maximum 20)"
        Timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        ReadingValue = Get-RandomDouble -Min $r[0] -Max $r[1]
        MeasurementUnit = $units[$type]
        SensorType = $type
        QualityFlag = @("Good","Good","Good","Suspect") | Get-Random
        IsAnomaly = ((Get-Random -Maximum 100) -lt 3)
    }
}

function New-ManufacturingEvent {
    $sensorTypes = @("Vibration","Temperature","Pressure","Speed","Current","OilPressure")
    $units = @{ Vibration="mm/s"; Temperature="Celsius"; Pressure="Bar"; Speed="RPM"; Current="Amps"; OilPressure="Bar" }
    $ranges = @{ Vibration=@(0.1,12); Temperature=@(20,120); Pressure=@(1,20); Speed=@(100,5000); Current=@(5,80); OilPressure=@(1,8) }
    $type = $sensorTypes | Get-Random
    $r = $ranges[$type]
    return @{
        SensorId = "SEN-$(Get-Random -Minimum 1 -Maximum 60)"
        MachineId = "MCH-$(Get-Random -Minimum 1 -Maximum 25)"
        LineId = "LINE-$(Get-Random -Minimum 1 -Maximum 10)"
        PlantId = "PLT-$(Get-Random -Minimum 1 -Maximum 5)"
        Timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        ReadingValue = Get-RandomDouble -Min $r[0] -Max $r[1]
        MeasurementUnit = $units[$type]
        SensorType = $type
        QualityFlag = @("Good","Good","Good","Suspect","Bad") | Get-Random
        IsAnomaly = ((Get-Random -Maximum 100) -lt 4)
    }
}

function New-ITAssetEvent {
    return @{
        ServerId = "SRV-$(Get-Random -Minimum 1 -Maximum 40)"
        RackId = "RACK-$(Get-Random -Minimum 1 -Maximum 20)"
        DataCenterId = "DC-$(Get-Random -Minimum 1 -Maximum 5)"
        Timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        CPUPercent = Get-RandomDouble -Min 5 -Max 98
        MemoryPercent = Get-RandomDouble -Min 15 -Max 95
        DiskIOPS = [int](Get-RandomDouble -Min 50 -Max 8000)
        NetworkMbps = Get-RandomDouble -Min 0.5 -Max 950
        QualityFlag = @("Good","Good","Good","Suspect") | Get-Random
        IsAnomaly = ((Get-Random -Maximum 100) -lt 5)
    }
}

function New-WindTurbineEvent {
    $windSpeed = Get-RandomDouble -Min 2 -Max 28
    $powerCurve = if ($windSpeed -lt 3) { 0 } elseif ($windSpeed -gt 25) { 0 } elseif ($windSpeed -gt 12) { 5000 } else { [math]::Round(5000 * [math]::Pow($windSpeed / 12, 3), 0) }
    return @{
        TurbineId = "TRB-$(Get-Random -Minimum 1 -Maximum 30)"
        FarmId = "FARM-$(Get-Random -Minimum 1 -Maximum 5)"
        SensorId = "SEN-$(Get-Random -Minimum 1 -Maximum 60)"
        SensorType = @("Anemometer","PowerMeter","RotorEncoder","PitchSensor","YawEncoder","Accelerometer") | Get-Random
        Timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        Value = $powerCurve
        Unit = "kW"
        Quality = @("Good","Good","Good","Suspect") | Get-Random
        IsAnomaly = ((Get-Random -Maximum 100) -lt 4)
    }
}

function New-HealthcareEvent {
    $patientId  = "PAT-$('{0:D3}' -f (Get-Random -Minimum 1 -Maximum 26))"
    $wardId     = "WD-$('{0:D3}' -f (Get-Random -Minimum 1 -Maximum 21))"
    $deptId     = "DEPT-$('{0:D3}' -f (Get-Random -Minimum 1 -Maximum 11))"
    $heartRate  = Get-RandomDouble -Min 55 -Max 130
    $isAnomaly  = ((Get-Random -Maximum 100) -lt 5)
    if ($isAnomaly) { $heartRate = Get-RandomDouble -Min 140 -Max 180 }
    return @{
        PatientId             = $patientId
        WardId                = $wardId
        DepartmentId          = $deptId
        Timestamp             = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        HeartRateBPM          = $heartRate
        BloodPressureSystolic = [int](Get-RandomDouble -Min 90 -Max 165)
        BloodPressureDiastolic= [int](Get-RandomDouble -Min 55 -Max 100)
        TemperatureC          = Get-RandomDouble -Min 36.0 -Max 38.8
        OxygenSaturation      = Get-RandomDouble -Min 88 -Max 100
        RespiratoryRate       = [int](Get-RandomDouble -Min 12 -Max 26)
        QualityFlag           = @("Good","Good","Good","Suspect") | Get-Random
        IsAnomaly             = $isAnomaly
    }
}

# ── Event generator dispatcher ──────────────────────────────────────────────
$generators = @{
    OilGasRefinery     = { New-OilGasEvent }
    SmartBuilding      = { New-SmartBuildingEvent }
    ManufacturingPlant = { New-ManufacturingEvent }
    ITAsset            = { New-ITAssetEvent }
    WindTurbine        = { New-WindTurbineEvent }
    Healthcare         = { New-HealthcareEvent }
}

$generator = $generators[$OntologyType]
if (-not $generator) {
    Write-Host "[ERROR] Unknown OntologyType: $OntologyType" -ForegroundColor Red
    exit 1
}

# ── Parse connection string ─────────────────────────────────────────────────
# Format: Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<name>;SharedAccessKey=<key>;EntityPath=<ehname>
$csParams = @{}
foreach ($part in $ConnectionString.Split(';')) {
    $kv = $part.Split('=', 2)
    if ($kv.Count -eq 2) { $csParams[$kv[0].Trim()] = $kv[1].Trim() }
}

$endpoint = $csParams["Endpoint"] -replace "^sb://", "https://"
$endpoint = $endpoint.TrimEnd('/')
$entityPath = $csParams["EntityPath"]
$keyName = $csParams["SharedAccessKeyName"]
$key = $csParams["SharedAccessKey"]

if (-not $endpoint -or -not $entityPath -or -not $keyName -or -not $key) {
    Write-Host "[ERROR] Invalid connection string. Expected Event Hub-compatible format." -ForegroundColor Red
    Write-Host "  Get it from: Fabric > Eventstream > Custom App source > Keys" -ForegroundColor Yellow
    exit 1
}

# ── Generate SAS Token ──────────────────────────────────────────────────────
function New-SASToken {
    param([string]$Uri, [string]$KeyName, [string]$Key, [int]$ExpirySeconds = 3600)
    $sinceEpoch = [int](([DateTime]::UtcNow - [DateTime]::new(1970,1,1,0,0,0,0,'Utc')).TotalSeconds)
    $expiry = $sinceEpoch + $ExpirySeconds
    $encodedUri = [System.Web.HttpUtility]::UrlEncode($Uri)
    $stringToSign = "$encodedUri`n$expiry"
    $hmac = New-Object System.Security.Cryptography.HMACSHA256
    $hmac.Key = [Text.Encoding]::UTF8.GetBytes($Key)
    $signature = [Convert]::ToBase64String($hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($stringToSign)))
    $encodedSig = [System.Web.HttpUtility]::UrlEncode($signature)
    return "SharedAccessSignature sr=$encodedUri&sig=$encodedSig&se=$expiry&skn=$KeyName"
}

Add-Type -AssemblyName System.Web

$sendUri = "$endpoint/$entityPath"
$sasToken = New-SASToken -Uri $sendUri -KeyName $keyName -Key $key

# ── Send events ─────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== Telemetry Simulator ===" -ForegroundColor Cyan
Write-Host "  Domain:     $OntologyType"
Write-Host "  Target:     $entityPath"
Write-Host "  Events:     $EventCount"
Write-Host "  Interval:   ${IntervalMs}ms"
Write-Host "  Batch size: $BatchSize"
Write-Host ""
Write-Host "Sending events..." -ForegroundColor Yellow

$sendHeaders = @{
    "Authorization" = $sasToken
    "Content-Type"  = "application/atom+xml;type=entry;charset=utf-8"
}

$sent = 0
$errors = 0
$startTime = Get-Date

while ($sent -lt $EventCount) {
    $batchEvents = @()
    $batchCount = [math]::Min($BatchSize, $EventCount - $sent)

    for ($b = 0; $b -lt $batchCount; $b++) {
        $event = & $generator
        $batchEvents += ($event | ConvertTo-Json -Compress)
    }

    foreach ($eventJson in $batchEvents) {
        try {
            Invoke-RestMethod -Method Post -Uri "${sendUri}/messages" `
                -Headers $sendHeaders -Body $eventJson -UseBasicParsing | Out-Null
            $sent++
        }
        catch {
            $errors++
            if ($errors -le 3) {
                Write-Host "  [ERROR] Send failed: $($_.Exception.Message)" -ForegroundColor Red
            }
        }
    }

    # Progress
    if ($sent % 50 -eq 0 -or $sent -eq $EventCount) {
        $elapsed = ((Get-Date) - $startTime).TotalSeconds
        $rate = if ($elapsed -gt 0) { [math]::Round($sent / $elapsed, 1) } else { 0 }
        Write-Host "  Sent: $sent / $EventCount  ($rate events/sec)  Errors: $errors" -ForegroundColor Gray
    }

    if ($sent -lt $EventCount) {
        Start-Sleep -Milliseconds $IntervalMs
    }
}

$totalTime = [math]::Round(((Get-Date) - $startTime).TotalSeconds, 1)

Write-Host ""
Write-Host "=== Simulation Complete ===" -ForegroundColor Cyan
Write-Host "  Events sent: $sent"
Write-Host "  Errors:      $errors"
Write-Host "  Duration:    ${totalTime}s"
Write-Host "  Avg rate:    $([math]::Round($sent / [math]::Max($totalTime, 0.1), 1)) events/sec"
Write-Host ""
Write-Host "Check the KQL Database for ingested events." -ForegroundColor White
Write-Host "=== Done ===" -ForegroundColor Cyan
