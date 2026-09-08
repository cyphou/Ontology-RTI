<#
.SYNOPSIS
  M365 Cowork pipeline for Enterprise Finance + HR: asks business questions against the
  semantic model, computes a simple forecast, and writes a structured insights JSON that
  feeds the PPTX deck generator and the Teams meeting scheduler.

.DESCRIPTION
  1. "Ask" a set of business questions as DAX queries (Copilot/Q&A style) against the
     Direct Lake model.
  2. Pull the Actual-spend and Headcount time series and compute a linear-trend forecast
     for the next N fiscal periods (simple least-squares regression, no external deps).
  3. Write artifacts/cowork-insights.json consumed by generate-insights-deck.mjs.
#>
[CmdletBinding()]
param(
    [string]$SemanticModelId = '398930b6-ed17-4371-bfcd-d4e352e0c7b1',
    [int]$ForecastPeriods = 3,
    [string]$OutFile = 'C:\GitHub Project\OntologyAccelerator\artifacts\cowork-insights.json'
)

$ErrorActionPreference = 'Stop'
$tok = (Get-AzAccessToken -ResourceUrl 'https://analysis.windows.net/powerbi/api' -WarningAction SilentlyContinue).Token
$h = @{ Authorization = "Bearer $tok"; 'Content-Type' = 'application/json' }

function Invoke-Dax([string]$dax) {
    $body = @{ queries = @(@{ query = $dax }); serializerSettings = @{ includeNulls = $true } } | ConvertTo-Json -Depth 10
    $r = Invoke-RestMethod -Method Post -Uri "https://api.powerbi.com/v1.0/myorg/datasets/$SemanticModelId/executeQueries" -Headers $h -Body $body
    return $r.results[0].tables[0].rows
}

Write-Host 'Asking business questions against the model...' -ForegroundColor Cyan

# --- "Ask Copilot" style Q&A: natural-language questions mapped to DAX ---
$questions = [ordered]@{}
function Money([double]$v) { '{0:N0}' -f $v }

$row = (Invoke-Dax 'EVALUATE ROW("Actual",[Actual],"Budget",[Budget],"Variance",[Variance],"VariancePct",[Variance %])')[0]
$questions['How are we tracking against budget?'] = 'Actual spend is ${0} vs a budget of ${1}, a variance of ${2} ({3}%).' -f (Money $row.'[Actual]'), (Money $row.'[Budget]'), (Money $row.'[Variance]'), [Math]::Round($row.'[VariancePct]' * 100, 1)

$row = (Invoke-Dax 'EVALUATE ROW("Headcount",[Headcount],"FTE",[FTE])')[0]
$questions['What is our current workforce size?'] = "Headcount is $([Math]::Round($row.'[Headcount]',0)) ($([Math]::Round($row.'[FTE]',0)) FTE)."

$row = (Invoke-Dax 'EVALUATE ROW("Offers",[Offers],"NewHires",[New Hires],"Accept",[Acceptance Rate],"OpenReq",[Open Requisitions])')[0]
$questions['How is recruitment performing?'] = "$([Math]::Round($row.'[Offers]',0)) offers extended, $([Math]::Round($row.'[NewHires]',0)) new hires, $([Math]::Round($row.'[Accept]'*100,1))% acceptance rate, with $([Math]::Round($row.'[OpenReq]',0)) open requisitions."

$row = (Invoke-Dax 'EVALUATE ROW("TC",[Total Compensation],"Base",[Base Salary],"Payroll",[Payroll Cost per FTE])')[0]
$baseTxt = if ($null -ne $row.'[Base]') { '${0} base salary' -f (Money $row.'[Base]') } else { 'base salary not yet posted' }
$questions['What is driving compensation cost?'] = 'Total compensation is ${0} ({1}), or ${2} payroll cost per FTE.' -f (Money $row.'[TC]'), $baseTxt, (Money $row.'[Payroll]')

$row = (Invoke-Dax 'EVALUATE ROW("Att",[Attendance Rate],"OT",[Overtime Rate],"AbsR",[Absence Rate])')[0]
$questions['How is workforce attendance trending?'] = "Attendance rate is $([Math]::Round($row.'[Att]'*100,1))%, overtime rate $([Math]::Round($row.'[OT]'*100,1))%, absence rate $([Math]::Round($row.'[AbsR]'*100,1))%."

Write-Host 'Pulling time series for forecasting...' -ForegroundColor Cyan

# --- Forecast: linear trend on Actual spend and Headcount by fiscal period ---
$series = Invoke-Dax @'
EVALUATE
  SUMMARIZECOLUMNS(
    dimfiscalperiod[PeriodName], dimfiscalperiod[PeriodStartDate],
    "Actual", [Actual], "Headcount", [Headcount]
  )
ORDER BY [PeriodStartDate]
'@

function Get-LinearForecast([array]$values, [int]$periodsAhead) {
    $n = $values.Count
    $xs = 0..($n - 1)
    $sumX = ($xs | Measure-Object -Sum).Sum
    $sumY = ($values | Measure-Object -Sum).Sum
    $sumXY = 0; $sumX2 = 0
    for ($i = 0; $i -lt $n; $i++) { $sumXY += $xs[$i] * $values[$i]; $sumX2 += $xs[$i] * $xs[$i] }
    $slope = (($n * $sumXY) - ($sumX * $sumY)) / (($n * $sumX2) - ($sumX * $sumX))
    $intercept = ($sumY - ($slope * $sumX)) / $n
    $forecast = @()
    for ($i = 1; $i -le $periodsAhead; $i++) { $forecast += [Math]::Round($intercept + ($slope * ($n - 1 + $i)), 1) }
    return @{ slope = [Math]::Round($slope, 3); forecast = $forecast }
}

$actualSeries = $series | ForEach-Object { [double]$_.'[Actual]' }
$headcountSeries = $series | ForEach-Object { [double]$_.'[Headcount]' }
$periodLabels = $series | ForEach-Object { $_.'[PeriodName]' }

$actualForecast = Get-LinearForecast $actualSeries $ForecastPeriods
$headcountForecast = Get-LinearForecast $headcountSeries $ForecastPeriods

$trendWord = if ($actualForecast.slope -gt 0) { 'rising' } else { 'declining' }
$forecastStr = ($actualForecast.forecast | ForEach-Object { '${0:N0}' -f $_ }) -join ', '
$questions['Where is spend trending over the next quarter?'] = 'Actual spend is {0} (${1}/period trend); linear forecast for the next {2} periods: {3}.' -f $trendWord, [Math]::Round($actualForecast.slope, 0), $ForecastPeriods, $forecastStr

$insights = [ordered]@{
    generatedAt = (Get-Date).ToString('o')
    model       = 'EnterpriseFinanceHRModel'
    questions   = $questions
    forecast    = [ordered]@{
        periodLabels     = $periodLabels
        actualHistory    = $actualSeries
        actualSlope      = $actualForecast.slope
        actualForecast   = $actualForecast.forecast
        headcountHistory = $headcountSeries
        headcountSlope   = $headcountForecast.slope
        headcountForecast = $headcountForecast.forecast
        forecastPeriods  = $ForecastPeriods
    }
}

New-Item -ItemType Directory -Force -Path (Split-Path $OutFile -Parent) | Out-Null
$insights | ConvertTo-Json -Depth 10 | Set-Content -Path $OutFile -Encoding UTF8
Write-Host "Insights written to: $OutFile" -ForegroundColor Green
$insights.questions.GetEnumerator() | ForEach-Object { Write-Host "Q: $($_.Key)`n   A: $($_.Value)" -ForegroundColor Yellow }
