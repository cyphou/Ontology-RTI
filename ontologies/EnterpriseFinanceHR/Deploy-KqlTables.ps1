param([Parameter(Mandatory=$true)][string]$WorkspaceId, [string]$KqlDatabaseId, [string]$QueryServiceUri, [string]$KqlDatabaseName = 'EnterpriseFinanceHREH')
$ErrorActionPreference = 'Stop'
$fabricToken = (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com').Token
$headers = @{ Authorization = "Bearer $fabricToken"; 'Content-Type' = 'application/json' }; $baseUri = 'https://api.fabric.microsoft.com/v1'
if (-not $KqlDatabaseId) { $database = ((Invoke-RestMethod -Uri "$baseUri/workspaces/$WorkspaceId/items?type=KQLDatabase" -Headers $headers).value | Where-Object { $_.displayName -eq $KqlDatabaseName } | Select-Object -First 1); if (-not $database) { throw "KQL database '$KqlDatabaseName' was not found." }; $KqlDatabaseId = $database.id }
if (-not $QueryServiceUri) { $details = Invoke-RestMethod -Uri "$baseUri/workspaces/$WorkspaceId/kqlDatabases/$KqlDatabaseId" -Headers $headers; $QueryServiceUri = $details.properties.queryServiceUri }
if (-not $QueryServiceUri) { throw 'The KQL query service URI could not be resolved.' }
$kustoToken = (Get-AzAccessToken -ResourceUrl $QueryServiceUri).Token
function Invoke-Kusto([string]$Command) { $body = @{ db = $KqlDatabaseName; csl = $Command } | ConvertTo-Json; Invoke-RestMethod -Method Post -Uri "$QueryServiceUri/v1/rest/mgmt" -Headers @{ Authorization = "Bearer $kustoToken"; 'Content-Type' = 'application/json' } -Body $body }
$tables = @(
    @{ Name = 'FinanceVariance'; Schema = '(Timestamp:datetime,FiscalPeriodId:string,CostCenterId:string,AccountId:string,BudgetAmount:real,ActualAmount:real,VarianceAmount:real)' },
    @{ Name = 'PayrollCostAnomaly'; Schema = '(Timestamp:datetime,FiscalPeriodId:string,CostCenterId:string,PlannedCompensation:real,DeviationPercent:real,Severity:string)' },
    @{ Name = 'WorkforceMovement'; Schema = '(Timestamp:datetime,FiscalPeriodId:string,DepartmentId:string,MovementType:string,MovementCount:real)' },
    @{ Name = 'ForecastFreshness'; Schema = '(Timestamp:datetime,FiscalPeriodId:string,CostCenterId:string,ScenarioName:string,ForecastAmount:real,AgeHours:real)' },
    @{ Name = 'PlanningException'; Schema = '(Timestamp:datetime,ExceptionType:string,EntityId:string,Severity:string,Message:string)' }
)
foreach ($table in $tables) { Invoke-Kusto ".create-merge table $($table.Name) $($table.Schema)" | Out-Null; Invoke-Kusto ".alter table $($table.Name) policy streamingingestion '{\"IsEnabled\":true}'" | Out-Null }
Write-Host "Created $($tables.Count) Enterprise Finance + HR KQL tables." -ForegroundColor Green