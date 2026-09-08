<#
.SYNOPSIS
    Creates or updates the Enterprise Finance + HR BI Dataflow Gen2.
.DESCRIPTION
    Builds curated, aggregate-only BI tables from the EnterpriseFinanceHRLH
    Lakehouse. The source tables are loaded by EnterpriseFinanceHR_LoadTables.
    The dataflow writes bi_cost_center_monthly, bi_forecast_variance, and
    bi_workforce_monthly back to the same Lakehouse.

    A Lakehouse connection and its Power BI gateway ClusterId must be supplied.
    The script never creates a credential-bearing connection or falls back to a
    different Lakehouse.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$WorkspaceId,
    [Parameter(Mandatory = $true)][string]$LakehouseId,
    [Parameter(Mandatory = $true)][string]$LakehouseConnectionId,
    [Parameter(Mandatory = $true)][string]$GatewayClusterId,
    [string]$DataflowName = 'EnterpriseFinanceHR BI Curated',
    [switch]$SkipRefresh
)

$ErrorActionPreference = 'Stop'
$apiBase = 'https://api.fabric.microsoft.com/v1'
$token = (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com').Token
$headers = @{ Authorization = "Bearer $token"; 'Content-Type' = 'application/json' }

function To-Base64([string]$Value) {
    [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Value))
}

function Wait-FabricOperation($Response) {
    if ($Response.StatusCode -ne 202) { return }
    $location = $Response.Headers['Location']
    if ($location -is [array]) { $location = $location[0] }
    $retryAfter = 5
    $retryValue = $Response.Headers['Retry-After']
    if ($retryValue -is [array]) { $retryValue = $retryValue[0] }
    [void][int]::TryParse([string]$retryValue, [ref]$retryAfter)
    do {
        Start-Sleep -Seconds $retryAfter
        $operation = Invoke-RestMethod -Method Get -Uri $location -Headers $headers
    } while ($operation.status -notin @('Succeeded', 'Failed', 'Cancelled'))
    if ($operation.status -ne 'Succeeded') { throw "Fabric operation $($operation.status): $($operation.error.message)" }
}

$lakehouse = ((Invoke-RestMethod -Method Get -Uri "$apiBase/workspaces/$WorkspaceId/items?type=Lakehouse" -Headers $headers).value |
    Where-Object { $_.id -eq $LakehouseId -and $_.displayName -eq 'EnterpriseFinanceHRLH' } |
    Select-Object -First 1)
if (-not $lakehouse) { throw "Lakehouse '$LakehouseId' is not EnterpriseFinanceHRLH in workspace '$WorkspaceId'." }

$connection = Invoke-RestMethod -Method Get -Uri "$apiBase/connections/$LakehouseConnectionId" -Headers $headers
if ($connection.connectionDetails.type -ne 'Lakehouse') { throw "Connection '$LakehouseConnectionId' is not a Lakehouse connection." }

$connectionId = (@{ ClusterId = $GatewayClusterId; DatasourceId = $LakehouseConnectionId } | ConvertTo-Json -Compress)
$queryId = @{
    CostCenterMonthly = [guid]::NewGuid().ToString()
    CostCenterMonthlyDestination = [guid]::NewGuid().ToString()
    ForecastVariance = [guid]::NewGuid().ToString()
    ForecastVarianceDestination = [guid]::NewGuid().ToString()
    WorkforceMonthly = [guid]::NewGuid().ToString()
    WorkforceMonthlyDestination = [guid]::NewGuid().ToString()
}

$destination = @"
    Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
    Workspace = Pattern{[workspaceId = "$WorkspaceId"]}[Data],
    Lakehouse = Workspace{[lakehouseId = "$LakehouseId"]}[Data],
    Target = Lakehouse{[Id = "__TARGET_TABLE__", ItemKind = "Table"]}?[Data]?
in
    Target;
"@

$mashup = @"
section Section1;

shared FinanceBudget = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false]){[workspaceId = "$WorkspaceId"]}[Data]{[lakehouseId = "$LakehouseId"]}[Data]{[Id = "factbudgetplan", ItemKind = "Table"]}[Data];
shared FinanceActual = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false]){[workspaceId = "$WorkspaceId"]}[Data]{[lakehouseId = "$LakehouseId"]}[Data]{[Id = "factactualledger", ItemKind = "Table"]}[Data];
shared FinanceForecast = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false]){[workspaceId = "$WorkspaceId"]}[Data]{[lakehouseId = "$LakehouseId"]}[Data]{[Id = "factforecastscenario", ItemKind = "Table"]}[Data];
shared WorkforceSnapshot = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false]){[workspaceId = "$WorkspaceId"]}[Data]{[lakehouseId = "$LakehouseId"]}[Data]{[Id = "factheadcountsnapshot", ItemKind = "Table"]}[Data];

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "CostCenterMonthly_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared CostCenterMonthly = let
    Budget = Table.Group(FinanceBudget, {"FiscalPeriodId", "CostCenterId"}, {{"BudgetAmount", each List.Sum([BudgetAmount]), type number}}),
    Actual = Table.Group(FinanceActual, {"FiscalPeriodId", "CostCenterId"}, {{"ActualAmount", each List.Sum([ActualAmount]), type number}}),
    Joined = Table.NestedJoin(Budget, {"FiscalPeriodId", "CostCenterId"}, Actual, {"FiscalPeriodId", "CostCenterId"}, "Actual", JoinKind.LeftOuter),
    Expanded = Table.ExpandTableColumn(Joined, "Actual", {"ActualAmount"}, {"ActualAmount"}),
    Typed = Table.TransformColumnTypes(Expanded, {{"BudgetAmount", type number}, {"ActualAmount", type number}}),
    Result = Table.AddColumn(Typed, "VarianceAmount", each [ActualAmount] - [BudgetAmount], type number)
in
    Result;
shared CostCenterMonthly_DataDestination = let
$(($destination.Replace('__TARGET_TABLE__', 'bi_cost_center_monthly')))

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ForecastVariance_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared ForecastVariance = let
    Forecast = Table.SelectRows(FinanceForecast, each [ScenarioName] = "Synthetic Base"),
    Budget = Table.Group(FinanceBudget, {"FiscalPeriodId", "CostCenterId"}, {{"BudgetAmount", each List.Sum([BudgetAmount]), type number}}),
    Joined = Table.NestedJoin(Forecast, {"FiscalPeriodId", "CostCenterId"}, Budget, {"FiscalPeriodId", "CostCenterId"}, "Budget", JoinKind.LeftOuter),
    Expanded = Table.ExpandTableColumn(Joined, "Budget", {"BudgetAmount"}, {"BudgetAmount"}),
    Result = Table.AddColumn(Expanded, "ForecastVarianceAmount", each [ForecastAmount] - [BudgetAmount], type number)
in
    Result;
shared ForecastVariance_DataDestination = let
$(($destination.Replace('__TARGET_TABLE__', 'bi_forecast_variance')))

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "WorkforceMonthly_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared WorkforceMonthly = let
    Grouped = Table.Group(WorkforceSnapshot, {"FiscalPeriodId", "DepartmentId"}, {{"Headcount", each List.Sum([Headcount]), type number}, {"Fte", each List.Sum([Fte]), type number}, {"OpenPositions", each List.Sum([OpenPositions]), type number}}),
    Result = Table.AddColumn(Grouped, "VacancyRate", each if [Headcount] + [OpenPositions] = 0 then 0 else [OpenPositions] / ([Headcount] + [OpenPositions]), Percentage.Type)
in
    Result;
shared WorkforceMonthly_DataDestination = let
$(($destination.Replace('__TARGET_TABLE__', 'bi_workforce_monthly')))
"@

$metadata = @{
    formatVersion = '202502'
    computeEngineSettings = @{ allowFastCopy = $true; maxConcurrency = 1 }
    name = $DataflowName
    queryGroups = @()
    documentLocale = 'en-US'
    queriesMetadata = @{
        CostCenterMonthly = @{ queryId = $queryId.CostCenterMonthly; queryName = 'CostCenterMonthly'; loadEnabled = $true }
        CostCenterMonthly_DataDestination = @{ queryId = $queryId.CostCenterMonthlyDestination; queryName = 'CostCenterMonthly_DataDestination'; isHidden = $true; loadEnabled = $false }
        ForecastVariance = @{ queryId = $queryId.ForecastVariance; queryName = 'ForecastVariance'; loadEnabled = $true }
        ForecastVariance_DataDestination = @{ queryId = $queryId.ForecastVarianceDestination; queryName = 'ForecastVariance_DataDestination'; isHidden = $true; loadEnabled = $false }
        WorkforceMonthly = @{ queryId = $queryId.WorkforceMonthly; queryName = 'WorkforceMonthly'; loadEnabled = $true }
        WorkforceMonthly_DataDestination = @{ queryId = $queryId.WorkforceMonthlyDestination; queryName = 'WorkforceMonthly_DataDestination'; isHidden = $true; loadEnabled = $false }
    }
    connections = @(@{ connectionId = $connectionId; kind = 'Lakehouse'; path = 'Lakehouse' })
    fastCombine = $false
    allowNativeQueries = $false
    parametric = $false
} | ConvertTo-Json -Depth 10 -Compress

$platform = (@{
    '$schema' = 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json'
    metadata = @{ type = 'Dataflow'; displayName = $DataflowName }
    config = @{ version = '2.0'; logicalId = [guid]::NewGuid().ToString() }
} | ConvertTo-Json -Depth 8 -Compress)

$definition = @{ parts = @(
    @{ path = 'queryMetadata.json'; payload = To-Base64 $metadata; payloadType = 'InlineBase64' },
    @{ path = 'mashup.pq'; payload = To-Base64 $mashup; payloadType = 'InlineBase64' },
    @{ path = '.platform'; payload = To-Base64 $platform; payloadType = 'InlineBase64' }
) }

$existing = ((Invoke-RestMethod -Method Get -Uri "$apiBase/workspaces/$WorkspaceId/dataflows" -Headers $headers).value | Where-Object { $_.displayName -eq $DataflowName } | Select-Object -First 1)
if ($existing) {
    $response = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/dataflows/$($existing.id)/updateDefinition?updateMetadata=true" -Headers $headers -Body (@{ definition = $definition } | ConvertTo-Json -Depth 20) -UseBasicParsing
    $dataflowId = $existing.id
} else {
    $response = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/dataflows" -Headers $headers -Body (@{ displayName = $DataflowName; definition = $definition } | ConvertTo-Json -Depth 20) -UseBasicParsing
    $dataflowId = ($response.Content | ConvertFrom-Json).id
}
Wait-FabricOperation $response

if (-not $SkipRefresh) {
    $refresh = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/dataflows/$dataflowId/jobs/instances?jobType=Refresh" -Headers $headers -Body '{"executionData":{"executeOption":"ApplyChangesIfNeeded"}}' -UseBasicParsing
    Wait-FabricOperation $refresh
}

Write-Host "Dataflow '$DataflowName' is configured (ID: $dataflowId)." -ForegroundColor Green