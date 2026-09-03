<#
.SYNOPSIS
    Publishes refinery incident-enrichment lookup tables and a KQL query function.
.DESCRIPTION
    Builds the governed join path required by operational clients:
      EquipmentAlert -> SensorLookup -> EquipmentLookup -> ProcessUnitLookup

    The output KQL function `EnrichedEquipmentAlert` returns alert, sensor, equipment,
    process-unit and refinery context without placing KQL credentials in a browser app.
#>
param(
    [Parameter(Mandatory=$true)] [string]$WorkspaceId,
    [Parameter(Mandatory=$true)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$true)] [string]$QueryServiceUri,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseName = "RefineryTelemetryEH",
    [Parameter(Mandatory=$false)] [string]$DataFolder = (Join-Path $PSScriptRoot "data")
)

$ErrorActionPreference = "Stop"
$kustoToken = (Get-AzAccessToken -ResourceUrl $QueryServiceUri).Token
$headers = @{ "Authorization" = "Bearer $kustoToken"; "Content-Type" = "application/json; charset=utf-8" }

function Invoke-KqlMgmt {
    param([Parameter(Mandatory=$true)][string]$Command)
    Write-Host "  KQL: $($Command.Split([Environment]::NewLine)[0])" -ForegroundColor DarkGray
    $body = @{ db = $KqlDatabaseName; csl = $Command } | ConvertTo-Json -Depth 5
    Invoke-RestMethod -Method Post -Uri "$QueryServiceUri/v1/rest/mgmt" -Headers $headers -Body ([System.Text.Encoding]::UTF8.GetBytes($body)) -ContentType "application/json; charset=utf-8"
}

function Invoke-KqlQuery {
    param([Parameter(Mandatory=$true)][string]$Query)
    $body = @{ db = $KqlDatabaseName; csl = $Query } | ConvertTo-Json -Depth 5
    Invoke-RestMethod -Method Post -Uri "$QueryServiceUri/v2/rest/query" -Headers $headers -Body ([System.Text.Encoding]::UTF8.GetBytes($body)) -ContentType "application/json; charset=utf-8"
}

function Get-KqlScalar {
    param([Parameter(Mandatory=$true)]$Response)
    # Kusto v2 REST returns a stream of frames. The PrimaryResult DataTable holds
    # query rows; management responses use a different shape and are not used here.
    $table = @($Response | Where-Object { $_.FrameType -eq "DataTable" -and $_.TableKind -eq "PrimaryResult" }) | Select-Object -First 1
    if ($table -and $table.Rows -and $table.Rows.Count -gt 0) {
        return $table.Rows[0][0]
    }
    return $null
}

function Convert-CsvToKqlRows {
    param([Parameter(Mandatory=$true)][string]$Path, [Parameter(Mandatory=$true)][string[]]$Columns)
    $rows = Import-Csv $Path
    return (($rows | ForEach-Object {
        $row = $_
        ($Columns | ForEach-Object {
            $column = $_
            $value = [string]$row.$column
            '"' + $value.Replace('"', '\"') + '"'
        }) -join ','
    }) -join "`n")
}

$tables = @(
    @{ Name = "SensorLookup"; Schema = "(SensorId:string, SensorName:string, SensorType:string, EquipmentId:string, MeasurementUnit:string)"; Csv = "DimSensor.csv"; Columns = @("SensorId","SensorName","SensorType","EquipmentId","MeasurementUnit") },
    @{ Name = "EquipmentLookup"; Schema = "(EquipmentId:string, EquipmentName:string, EquipmentType:string, ProcessUnitId:string, CriticalityLevel:string)"; Csv = "DimEquipment.csv"; Columns = @("EquipmentId","EquipmentName","EquipmentType","ProcessUnitId","CriticalityLevel") },
    @{ Name = "ProcessUnitLookup"; Schema = "(ProcessUnitId:string, ProcessUnitName:string, ProcessUnitType:string, RefineryId:string, CapacityBPD:real)"; Csv = "DimProcessUnit.csv"; Columns = @("ProcessUnitId","ProcessUnitName","ProcessUnitType","RefineryId","CapacityBPD") }
)

foreach ($table in $tables) {
    Invoke-KqlMgmt ".create-merge table $($table.Name) $($table.Schema)" | Out-Null
    $existing = Invoke-KqlQuery "$($table.Name) | count"
    $count = 0
    try { $count = [int](Get-KqlScalar $existing) } catch { }
    if ($count -eq 0) {
        $csvPath = Join-Path $DataFolder $table.Csv
        $rows = Convert-CsvToKqlRows -Path $csvPath -Columns $table.Columns
        if ($rows) {
            Invoke-KqlMgmt ".ingest inline into table $($table.Name) with (format='csv') <|`n$rows" | Out-Null
        }
    }
}

$function = @'
.create-or-alter function with (folder = "Operations", docstring = "Alert context joined to sensor, equipment, and process unit") EnrichedEquipmentAlert() {
    EquipmentAlert
    | join kind=leftouter (SensorLookup) on SensorId
    | join kind=leftouter (EquipmentLookup) on EquipmentId
    | join kind=leftouter (ProcessUnitLookup) on ProcessUnitId
    | project AlertId, Timestamp, Severity, IsAcknowledged, AlertType, Message,
              ReadingValue, ThresholdValue, SensorId, SensorName, SensorType, MeasurementUnit,
              EquipmentId, EquipmentName, EquipmentType, CriticalityLevel,
              ProcessUnitId, ProcessUnitName, ProcessUnitType, RefineryId, CapacityBPD
}
'@
Invoke-KqlMgmt $function | Out-Null

Write-Host "[OK] Published SensorLookup, EquipmentLookup, ProcessUnitLookup and EnrichedEquipmentAlert()." -ForegroundColor Green
Write-Host "     Query: EnrichedEquipmentAlert() | where IsAcknowledged == false | order by Timestamp desc" -ForegroundColor Gray
