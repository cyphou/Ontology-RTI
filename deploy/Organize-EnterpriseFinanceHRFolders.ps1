<#
.SYNOPSIS
  Idempotently organizes the Enterprise Finance + HR workspace into four folders.

.DESCRIPTION
  Moves business Fabric items into the existing 01 Data, 02 Planning,
  03 Analytics, and 04 Automation folders. Fabric system SQL items remain at
  the workspace root because they are managed by the platform.
#>
[CmdletBinding()]
param(
    [string]$WorkspaceId = '56b6ac57-a326-4c90-a040-36cabeec98ff'
)

$ErrorActionPreference = 'Stop'
$token = (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com' -WarningAction SilentlyContinue).Token
$headers = @{ Authorization = "Bearer $token"; 'Content-Type' = 'application/json' }
$base = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId"

$folders = (Invoke-RestMethod -Uri "$base/folders" -Headers $headers).value
$folderIds = @{}
$folders | ForEach-Object { $folderIds[$_.displayName] = $_.id }

$expectedFolders = @('01 Data', '02 Planning', '03 Analytics', '04 Automation')
foreach ($name in $expectedFolders) {
    if (-not $folderIds.ContainsKey($name)) {
        $folder = Invoke-RestMethod -Method Post -Uri "$base/folders" -Headers $headers -Body (@{ displayName = $name } | ConvertTo-Json)
        $folderIds[$name] = $folder.id
        Write-Host "Created folder: $name" -ForegroundColor Green
    }
}

$targetByName = @{
    'EnterpriseFinanceHR-DataAgent' = '04 Automation'
    'Enterprise Finance HR Aggregate Refresh' = '04 Automation'
    'EnterpriseFinanceHRPlanningStream' = '01 Data'
    'StagingLakehouseForDataflows_20260907152857' = '01 Data'
    'StagingWarehouseForDataflows_20260907152857' = '01 Data'
    'EnterpriseFinanceHR_QualityGate' = '04 Automation'
    'EnterpriseFinanceHRModel' = '02 Planning'
    'EnterpriseFinanceHREH' = '01 Data'
    'EnterpriseFinanceHRLH' = '01 Data'
    'EnterpriseFinanceHR_LoadTables' = '01 Data'
    'EnterpriseFinanceHROntology_graph_13458690d2304c6e91a6286955888db9' = '02 Planning'
    'EnterpriseFinanceHRQueries' = '02 Planning'
    'EnterpriseFinanceHROntology_lh_13458690d2304c6e91a6286955888db9' = '02 Planning'
    'EnterpriseFinanceHROntology' = '02 Planning'
    'Enterprise Finance + HR Plan' = '02 Planning'
    'enterprise-finance-hr' = '03 Analytics'
    'Enterprise HR Absence' = '03 Analytics'
    'Enterprise HR Attendance' = '03 Analytics'
    'Enterprise HR Compensation' = '03 Analytics'
    'Enterprise HR Recruitment' = '03 Analytics'
    'EnterpriseFinanceHR BI Curated' = '03 Analytics'
    'EnterpriseFinanceHRDashboard' = '03 Analytics'
    'EnterpriseFinanceHR-OperationsAgent' = '04 Automation'
}

$systemNames = @('__fabric_plan_sys')
$items = (Invoke-RestMethod -Uri "$base/items" -Headers $headers).value
$moved = 0
$rootSystem = 0
$unmapped = @()
foreach ($item in $items) {
    if ($item.folderId) { continue }
    if ($systemNames -contains $item.displayName) {
        $rootSystem++
        continue
    }

    $targetName = $targetByName[$item.displayName]
    if (-not $targetName) {
        $unmapped += "$($item.displayName) [$($item.type)]"
        continue
    }

    Invoke-RestMethod -Method Post -Uri "$base/items/$($item.id)/move" -Headers $headers -Body (@{ targetFolderId = $folderIds[$targetName] } | ConvertTo-Json) | Out-Null
    $moved++
}

Write-Host "Moved or confirmed business items: $moved" -ForegroundColor Green
Write-Host "System items left at root: $rootSystem" -ForegroundColor Gray
if ($unmapped.Count -gt 0) {
    Write-Warning "Unmapped root items: $($unmapped -join ', ')"
}
