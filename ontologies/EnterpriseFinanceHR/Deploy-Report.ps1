<#
.SYNOPSIS
  Generates and deploys a Power BI report in the modern PBIR (enhanced, folder-based)
  format, bound to the Enterprise Finance + HR Direct Lake semantic model.

.DESCRIPTION
  Emits the documented PBIR definition parts:
    definition.pbir
    definition/report.json
    definition/pages/pages.json
    definition/pages/<page>/page.json
    definition/pages/<page>/visuals/<visual>/visual.json
    .platform
  Each visual binds to real model measures / dimension columns (validated against the
  semantic model). The legacy report.json format is intentionally NOT used because the
  service renderer hangs on hand-authored legacy layouts.
#>
[CmdletBinding()]
param(
    [string]$WorkspaceId = '56b6ac57-a326-4c90-a040-36cabeec98ff',
    [string]$SemanticModelId = '398930b6-ed17-4371-bfcd-d4e352e0c7b1',
    [string]$ReportName = 'Enterprise Finance + HR Executive Report',
    [string]$WorkspaceName = '',
    [string]$ModelName = '',
    [string]$PbipOutDir = '',
    [string]$PbipName = 'EnterpriseFinanceHR',
    [switch]$Plain
)

$ErrorActionPreference = 'Stop'
$apiBase = 'https://api.fabric.microsoft.com/v1'
$token = (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com' -WarningAction SilentlyContinue).Token
$headers = @{ Authorization = "Bearer $token"; 'Content-Type' = 'application/json' }

# Resolve workspace + semantic model display names (required for the live connection string).
if (-not $WorkspaceName) {
    $WorkspaceName = (Invoke-RestMethod -Method Get -Uri "$apiBase/workspaces/$WorkspaceId" -Headers $headers).displayName
}
if (-not $ModelName) {
    $ModelName = ((Invoke-RestMethod -Method Get -Uri "$apiBase/workspaces/$WorkspaceId/items?type=SemanticModel" -Headers $headers).value |
        Where-Object { $_.id -eq $SemanticModelId } | Select-Object -First 1).displayName
}

$sbReport = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.1.0/schema.json'
$sbPage   = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json'
$sbVisual = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.6.0/schema.json'

function ToB64([object]$Obj) {
    $json = if ($Obj -is [string]) { $Obj } else { $Obj | ConvertTo-Json -Depth 40 }
    [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($json))
}
function NewName { [guid]::NewGuid().ToString('N').Substring(0, 20) }
function Lit([string]$v) { @{ expr = @{ Literal = @{ Value = $v } } } }
function Solid([string]$hex) { @{ solid = @{ color = @{ expr = @{ Literal = @{ Value = "'$hex'" } } } } } }

function Field-Measure([string]$entity, [string]$prop) {
    @{ Measure = @{ Expression = @{ SourceRef = @{ Entity = $entity } }; Property = $prop } }
}
function Field-Column([string]$entity, [string]$prop) {
    @{ Column = @{ Expression = @{ SourceRef = @{ Entity = $entity } }; Property = $prop } }
}

# Visual title only (matches the structure of known-working PBIR reports).
function VC-Objects([string]$title, [string]$bgHex) {
    @{
        title = @(@{ properties = @{
                    show = Lit 'true'
                    text = Lit "'$title'"
                } })
    }
}

function New-Visual([string]$page, [int]$x, [int]$y, [int]$w, [int]$h, [hashtable]$visual, [string]$title, [string]$bgHex) {
    $name = NewName
    if (-not $Plain) { $visual.visualContainerObjects = (VC-Objects $title $bgHex) }
    $visual.drillFilterOtherVisuals = $true
    $obj = [ordered]@{
        '$schema' = $sbVisual
        name      = $name
        position  = [ordered]@{ x = $x; y = $y; z = 0; width = $w; height = $h; tabOrder = 0 }
        visual    = $visual
    }
    return @{ path = "definition/pages/$page/visuals/$name/visual.json"; payload = (ToB64 $obj); payloadType = 'InlineBase64' }
}

function Card([string]$entity, [string]$measure) {
    @{
        visualType = 'card'
        query      = @{ queryState = @{ Values = @{ projections = @(
                        @{ field = (Field-Measure $entity $measure); queryRef = "$entity.$measure"; nativeQueryRef = $measure }
                    ) } } }
    }
}
function ColumnChart([string]$catEntity, [string]$catCol, [array]$measures) {
    $y = @(); foreach ($m in $measures) { $y += @{ field = (Field-Measure $m.Entity $m.Measure); queryRef = "$($m.Entity).$($m.Measure)"; nativeQueryRef = $m.Measure } }
    @{
        visualType = 'clusteredColumnChart'
        query      = @{ queryState = @{
                Category = @{ projections = @(@{ field = (Field-Column $catEntity $catCol); queryRef = "$catEntity.$catCol"; nativeQueryRef = $catCol }) }
                Y        = @{ projections = $y }
            } }
    }
}
function LineChart([string]$catEntity, [string]$catCol, [array]$measures) {
    $y = @(); foreach ($m in $measures) { $y += @{ field = (Field-Measure $m.Entity $m.Measure); queryRef = "$($m.Entity).$($m.Measure)"; nativeQueryRef = $m.Measure } }
    @{
        visualType = 'lineChart'
        query      = @{ queryState = @{
                Category = @{ projections = @(@{ field = (Field-Column $catEntity $catCol); queryRef = "$catEntity.$catCol"; nativeQueryRef = $catCol }) }
                Y        = @{ projections = $y }
            } }
    }
}
function BarChart([string]$catEntity, [string]$catCol, [array]$measures) {
    $y = @(); foreach ($m in $measures) { $y += @{ field = (Field-Measure $m.Entity $m.Measure); queryRef = "$($m.Entity).$($m.Measure)"; nativeQueryRef = $m.Measure } }
    @{
        visualType = 'clusteredBarChart'
        query      = @{ queryState = @{
                Category = @{ projections = @(@{ field = (Field-Column $catEntity $catCol); queryRef = "$catEntity.$catCol"; nativeQueryRef = $catCol }) }
                Y        = @{ projections = $y }
            } }
    }
}
function AreaChart([string]$catEntity, [string]$catCol, [array]$measures) {
    $y = @(); foreach ($m in $measures) { $y += @{ field = (Field-Measure $m.Entity $m.Measure); queryRef = "$($m.Entity).$($m.Measure)"; nativeQueryRef = $m.Measure } }
    @{
        visualType = 'areaChart'
        query      = @{ queryState = @{
                Category = @{ projections = @(@{ field = (Field-Column $catEntity $catCol); queryRef = "$catEntity.$catCol"; nativeQueryRef = $catCol }) }
                Y        = @{ projections = $y }
            } }
    }
}
function DonutChart([string]$catEntity, [string]$catCol, [hashtable]$measure) {
    @{
        visualType = 'donutChart'
        query      = @{ queryState = @{
                Category = @{ projections = @(@{ field = (Field-Column $catEntity $catCol); queryRef = "$catEntity.$catCol"; nativeQueryRef = $catCol }) }
                Values   = @{ projections = @(@{ field = (Field-Measure $measure.Entity $measure.Measure); queryRef = "$($measure.Entity).$($measure.Measure)"; nativeQueryRef = $measure.Measure }) }
            } }
    }
}
function Slicer([string]$entity, [string]$col) {
    @{
        visualType = 'slicer'
        query      = @{ queryState = @{ Values = @{ projections = @(@{ field = (Field-Column $entity $col); queryRef = "$entity.$col"; nativeQueryRef = $col }) } } }
    }
}
function Matrix([string]$rowEntity, [string]$rowCol, [array]$measures) {
    $vals = @(); foreach ($m in $measures) { $vals += @{ field = (Field-Measure $m.Entity $m.Measure); queryRef = "$($m.Entity).$($m.Measure)"; nativeQueryRef = $m.Measure } }
    @{
        visualType = 'pivotTable'
        query      = @{ queryState = @{
                Rows   = @{ projections = @(@{ field = (Field-Column $rowEntity $rowCol); queryRef = "$rowEntity.$rowCol"; nativeQueryRef = $rowCol }) }
                Values = @{ projections = $vals }
            } }
    }
}
function DecompTree([string]$aEntity, [string]$aMeasure, [array]$explain) {
    $exp = @(); $i = 0
    foreach ($e in $explain) { $exp += @{ field = (Field-Column $e.Entity $e.Column); queryRef = "$($e.Entity).$($e.Column)"; nativeQueryRef = $e.Column; active = ($i -eq 0) }; $i++ }
    @{
        visualType = 'decompositionTreeVisual'
        query      = @{ queryState = @{
                Analyze   = @{ projections = @(@{ field = (Field-Measure $aEntity $aMeasure); queryRef = "$aEntity.$aMeasure"; nativeQueryRef = $aMeasure }) }
                ExplainBy = @{ projections = $exp }
            } }
    }
}
function KeyInfluencers([string]$tEntity, [string]$tMeasure, [array]$explain) {
    $exp = @(); foreach ($e in $explain) { $exp += @{ field = (Field-Column $e.Entity $e.Column); queryRef = "$($e.Entity).$($e.Column)"; nativeQueryRef = $e.Column } }
    @{
        visualType = 'keyDriversVisual'
        query      = @{ queryState = @{
                Target    = @{ projections = @(@{ field = (Field-Measure $tEntity $tMeasure); queryRef = "$tEntity.$tMeasure"; nativeQueryRef = $tMeasure }) }
                ExplainBy = @{ projections = $exp }
            } }
    }
}

$fin = 'factactualledger'
$comp = 'factemployeecompensationsnapshot'
$req = 'factjobrequisition'

# --- Page 1: Executive Overview ---
$p1 = (NewName).Substring(0, 16)
$p1Visuals = @(
    (New-Visual $p1 16   16  300 120 (Card $fin 'Actual')   '💰 Actual Spend'   '#FFFFFF'),
    (New-Visual $p1 332  16  300 120 (Card $fin 'Budget')   '🎯 Budget'         '#FFFFFF'),
    (New-Visual $p1 648  16  300 120 (Card $fin 'Variance') '📉 Variance'       '#FFFFFF'),
    (New-Visual $p1 964  16  300 120 (Card $fin 'FTE')      '👥 Planned FTE'    '#FFFFFF'),
    (New-Visual $p1 16  152 620 280 (ColumnChart 'dimcostcenter' 'CostCenterName' @(@{Entity=$fin;Measure='Budget'}, @{Entity=$fin;Measure='Actual'})) '🏢 Budget vs Actual by Cost Center' '#FFFFFF'),
    (New-Visual $p1 648 152 616 280 (LineChart 'dimfiscalperiod' 'PeriodName' @(@{Entity=$fin;Measure='Actual'}, @{Entity=$fin;Measure='Forecast'})) '📈 Actual vs Forecast Over Time' '#FFFFFF'),
    (New-Visual $p1 16  448 300 256 (Slicer 'dimfiscalperiod' 'FiscalYear') '📅 Fiscal Year' '#FFFFFF'),
    (New-Visual $p1 332 448 932 256 (Matrix 'dimcostcenter' 'CostCenterName' @(@{Entity=$fin;Measure='Budget'}, @{Entity=$fin;Measure='Actual'}, @{Entity=$fin;Measure='Variance'}, @{Entity=$fin;Measure='FTE'})) '📊 Cost Center Detail' '#FFFFFF')
)

# --- Page 2: Workforce ---
$p2 = (NewName).Substring(0, 16)
$p2Visuals = @(
    (New-Visual $p2 16  16 300 120 (Card $fin 'Headcount')          '👥 Headcount'          '#FFFFFF'),
    (New-Visual $p2 332 16 300 120 (Card $fin 'FTE')                '👤 FTE'                '#FFFFFF'),
    (New-Visual $p2 648 16 300 120 (Card $comp 'Total Compensation') '💵 Total Compensation' '#FFFFFF'),
    (New-Visual $p2 964 16 300 120 (Card $req 'Openings Requested')  '📋 Open Demand'        '#FFFFFF'),
    (New-Visual $p2 16  152 620 280 (ColumnChart 'dimdepartment' 'DepartmentName' @(@{Entity=$fin;Measure='Headcount'})) '🏢 Headcount by Department' '#FFFFFF'),
    (New-Visual $p2 648 152 616 280 (ColumnChart 'dimcostcenter' 'CostCenterName' @(@{Entity=$comp;Measure='Total Compensation'})) '💵 Compensation by Cost Center' '#FFFFFF'),
    (New-Visual $p2 16 448 1248 256 (Matrix 'dimdepartment' 'DepartmentName' @(@{Entity=$fin;Measure='Headcount'}, @{Entity=$fin;Measure='FTE'}, @{Entity=$comp;Measure='Payroll Cost per FTE'})) '📊 Workforce Detail' '#FFFFFF')
)

$offer = 'factoffer'
$time = 'facttimeattendancedaily'
$abs = 'factabsenceepisode'

# --- Page 3: Recruitment ---
$p3 = (NewName).Substring(0, 16)
$p3Visuals = @(
    (New-Visual $p3 16   16  300 120 (Card $offer 'Offers')          '📨 Offers'            '#FFFFFF'),
    (New-Visual $p3 332  16  300 120 (Card $offer 'New Hires')       '✅ New Hires'         '#FFFFFF'),
    (New-Visual $p3 648  16  300 120 (Card $offer 'Acceptance Rate') '📈 Acceptance Rate'   '#FFFFFF'),
    (New-Visual $p3 964  16  300 120 (Card $req 'Open Requisitions') '📋 Open Requisitions' '#FFFFFF'),
    (New-Visual $p3 16  152 620 280 (BarChart 'dimrecruitmentstage' 'StageName' @(@{Entity='factrecruitmentstageevent';Measure='Active Candidates'})) '🪜 Candidates by Stage' '#FFFFFF'),
    (New-Visual $p3 648 152 616 280 (LineChart 'dimfiscalperiod' 'PeriodName' @(@{Entity=$offer;Measure='Offers'})) '📈 Offers Over Time' '#FFFFFF'),
    (New-Visual $p3 16  448 620 256 (ColumnChart 'dimdepartment' 'DepartmentName' @(@{Entity=$req;Measure='Open Requisitions'})) '🏢 Open Requisitions by Department' '#FFFFFF'),
    (New-Visual $p3 648 448 616 256 (Matrix 'dimdepartment' 'DepartmentName' @(@{Entity=$req;Measure='Open Requisitions'}, @{Entity=$req;Measure='Openings Requested'})) '📊 Requisition Detail' '#FFFFFF')
)

# --- Page 4: Compensation ---
$p4 = (NewName).Substring(0, 16)
$p4Visuals = @(
    (New-Visual $p4 16   16  300 120 (Card $comp 'Total Compensation')   '💵 Total Compensation' '#FFFFFF'),
    (New-Visual $p4 332  16  300 120 (Card $comp 'Base Salary')          '💶 Base Salary'        '#FFFFFF'),
    (New-Visual $p4 648  16  300 120 (Card $comp 'Payroll Cost per FTE') '🧮 Payroll / FTE'      '#FFFFFF'),
    (New-Visual $p4 964  16  300 120 (Card $fin 'FTE')                   '👥 FTE'                '#FFFFFF'),
    (New-Visual $p4 16  152 620 280 (ColumnChart 'dimjobfamily' 'JobFamilyName' @(@{Entity=$comp;Measure='Total Compensation'}, @{Entity=$comp;Measure='Base Salary'})) '👔 Compensation by Job Family' '#FFFFFF'),
    (New-Visual $p4 648 152 616 280 (BarChart 'dimpaygrade' 'PayGradeName' @(@{Entity=$comp;Measure='Payroll Cost per FTE'})) '🧮 Payroll / FTE by Pay Grade' '#FFFFFF'),
    (New-Visual $p4 16  448 620 256 (DonutChart 'dimcostcenter' 'CostCenterName' @{Entity=$comp;Measure='Total Compensation'}) '🍩 Compensation Mix by Cost Center' '#FFFFFF'),
    (New-Visual $p4 648 448 616 256 (Matrix 'dimjobfamily' 'JobFamilyName' @(@{Entity=$comp;Measure='Total Compensation'}, @{Entity=$comp;Measure='Base Salary'}, @{Entity=$comp;Measure='Payroll Cost per FTE'})) '📊 Compensation Detail' '#FFFFFF')
)

# --- Page 5: Time & Attendance ---
$p5 = (NewName).Substring(0, 16)
$p5Visuals = @(
    (New-Visual $p5 16   16  300 120 (Card $time 'Attendance Rate') '🟢 Attendance Rate' '#FFFFFF'),
    (New-Visual $p5 332  16  300 120 (Card $time 'Overtime Rate')   '🕒 Overtime Rate'   '#FFFFFF'),
    (New-Visual $p5 648  16  300 120 (Card $time 'Overtime Hours')  '🕘 Overtime Hours'  '#FFFFFF'),
    (New-Visual $p5 964  16  300 120 (Card $abs 'Absence Rate')     '🚫 Absence Rate'    '#FFFFFF'),
    (New-Visual $p5 16  152 620 280 (ColumnChart 'dimdepartment' 'DepartmentName' @(@{Entity=$time;Measure='Attendance Rate'}, @{Entity=$time;Measure='Overtime Rate'})) '🏢 Attendance vs Overtime by Department' '#FFFFFF'),
    (New-Visual $p5 648 152 616 280 (LineChart 'dimfiscalperiod' 'PeriodName' @(@{Entity=$time;Measure='Attendance Rate'})) '📈 Attendance Over Time' '#FFFFFF'),
    (New-Visual $p5 16  448 620 256 (BarChart 'dimleavetype' 'LeaveCategory' @(@{Entity=$abs;Measure='Absence Rate'}, @{Entity=$abs;Measure='Absence Days'})) '🌴 Absence by Leave Category' '#FFFFFF'),
    (New-Visual $p5 648 448 616 256 (Matrix 'dimdepartment' 'DepartmentName' @(@{Entity=$time;Measure='Attendance Rate'}, @{Entity=$time;Measure='Overtime Rate'}, @{Entity=$time;Measure='Overtime Hours'})) '📊 Attendance Detail' '#FFFFFF')
)

# --- Page 6: AI Insights (GenAI visuals) ---
$p6 = (NewName).Substring(0, 16)
$p6Visuals = @(
    (New-Visual $p6 16  16  624 340 (DecompTree $fin 'Actual' @(@{Entity='dimcostcenter';Column='CostCenterName'}, @{Entity='dimdepartment';Column='DepartmentName'}, @{Entity='dimfiscalperiod';Column='FiscalYear'})) '🌳 Spend Decomposition (AI splits)' '#FFFFFF'),
    (New-Visual $p6 648 16  616 340 (KeyInfluencers $fin 'Variance' @(@{Entity='dimcostcenter';Column='CostCenterName'}, @{Entity='dimfiscalperiod';Column='FiscalYear'})) '🧠 Key Influencers of Variance' '#FFFFFF'),
    (New-Visual $p6 16  372 1248 332 (Matrix 'dimcostcenter' 'CostCenterName' @(@{Entity=$fin;Measure='Actual'}, @{Entity=$fin;Measure='Budget'}, @{Entity=$fin;Measure='Variance'}, @{Entity=$fin;Measure='Variance %'})) '📊 Variance Explorer' '#FFFFFF')
)

function New-Page([string]$name, [string]$display) {
    return [ordered]@{
        '$schema'     = $sbPage
        name          = $name
        displayName   = $display
        displayOption = 'FitToPage'
        height        = 720
        width         = 1280
        objects       = @{
            background = @(@{ properties = @{ color = Solid '#F4F6FB'; transparency = Lit '0D' } })
            outspace   = @(@{ properties = @{ color = Solid '#0F172A'; transparency = Lit '0D' } })
        }
    }
}

# --- definition.pbir: modern live connection (matches Desktop/Fabric-authored reports) ---
$connStr = "Data Source=`"powerbi://api.powerbi.com/v1.0/myorg/$WorkspaceName`";initial catalog=$ModelName;access mode=readonly;integrated security=ClaimsToken;semanticmodelid=$SemanticModelId"
$pbir = [ordered]@{
    '$schema'        = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json'
    version          = '4.0'
    datasetReference = [ordered]@{
        byConnection = [ordered]@{ connectionString = $connStr }
    }
}

# --- Custom brand theme (shiny marketing look applied to every visual) ---
$themeName = 'EnterpriseFinanceHRTheme'
$rvi = [ordered]@{ visual = '2.6.0'; report = '3.1.0'; page = '2.3.0' }
$themeJson = @'
{
  "name": "EnterpriseFinanceHRTheme",
  "dataColors": ["#4F46E5","#06B6D4","#F59E0B","#EF4444","#10B981","#8B5CF6","#EC4899","#0EA5E9","#6366F1","#F97316"],
  "background": "#FFFFFF",
  "foreground": "#0F172A",
  "tableAccent": "#4F46E5",
  "good": "#10B981",
  "neutral": "#F59E0B",
  "bad": "#EF4444",
  "textClasses": {
    "callout": { "fontFace": "Segoe UI Semibold", "fontSize": 30, "color": "#4F46E5" },
    "title": { "fontFace": "Segoe UI Semibold", "fontSize": 13, "color": "#0F172A" },
    "header": { "fontFace": "Segoe UI Semibold", "fontSize": 10, "color": "#475569" },
    "label": { "fontFace": "Segoe UI", "fontSize": 10, "color": "#64748B" }
  },
  "visualStyles": {
    "*": {
      "*": {
        "background": [{ "show": true, "color": { "solid": { "color": "#FFFFFF" } }, "transparency": 0 }],
        "border": [{ "show": true, "color": { "solid": { "color": "#E9EDF5" } }, "radius": 14 }],
        "dropShadow": [{ "show": true, "preset": "BottomRight" }],
        "title": [{ "show": true, "fontColor": { "solid": { "color": "#0F172A" } }, "fontSize": 12, "bold": true, "fontFamily": "Segoe UI Semibold", "alignment": "left" }]
      }
    },
    "card": {
      "*": {
        "labels": [{ "color": { "solid": { "color": "#4F46E5" } }, "fontSize": 30, "fontFamily": "Segoe UI Semibold" }],
        "categoryLabels": [{ "show": true, "color": { "solid": { "color": "#64748B" } }, "fontSize": 11 }]
      }
    },
    "pivotTable": {
      "*": {
        "columnHeaders": [{ "fontColor": { "solid": { "color": "#FFFFFF" } }, "backColor": { "solid": { "color": "#4F46E5" } }, "bold": true }],
        "values": [{ "fontColorPrimary": { "solid": { "color": "#334155" } }, "backColorPrimary": { "solid": { "color": "#FFFFFF" } }, "backColorSecondary": { "solid": { "color": "#F4F6FB" } } }]
      }
    }
  }
}
'@

# --- definition/report.json ---
$report = [ordered]@{
    '$schema'          = $sbReport
    themeCollection    = [ordered]@{
        baseTheme   = [ordered]@{ name = 'CY26SU02'; reportVersionAtImport = $rvi; type = 'SharedResources' }
        customTheme = [ordered]@{ name = $themeName; reportVersionAtImport = $rvi; type = 'RegisteredResources' }
    }
    resourcePackages   = @(
        @{ name = 'SharedResources'; type = 'SharedResources'; items = @(@{ name = 'CY26SU02'; path = 'BaseThemes/CY26SU02.json'; type = 'BaseTheme' }) },
        @{ name = 'RegisteredResources'; type = 'RegisteredResources'; items = @(@{ name = "$themeName.json"; path = "$themeName.json"; type = 'CustomTheme' }) }
    )
    settings           = [ordered]@{ useStylableVisualContainerHeader = $true; defaultDrillFilterOtherVisuals = $true }
}

# --- definition/pages/pages.json ---
$pagesMeta = [ordered]@{
    '$schema'      = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json'
    pageOrder      = @($p1, $p2, $p3, $p4, $p5, $p6)
    activePageName = $p1
}

$platform = [ordered]@{
    '$schema' = 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json'
    metadata  = [ordered]@{ type = 'Report'; displayName = $ReportName }
    config    = [ordered]@{ version = '2.0'; logicalId = [guid]::NewGuid().ToString() }
}

$versionMeta = [ordered]@{
    '$schema' = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json'
    version   = '2.0.0'
}

$parts = @()
$parts += @{ path = 'definition.pbir'; payload = (ToB64 $pbir); payloadType = 'InlineBase64' }
$parts += @{ path = 'definition/version.json'; payload = (ToB64 $versionMeta); payloadType = 'InlineBase64' }
$parts += @{ path = 'definition/report.json'; payload = (ToB64 $report); payloadType = 'InlineBase64' }
$parts += @{ path = "StaticResources/RegisteredResources/$themeName.json"; payload = (ToB64 $themeJson); payloadType = 'InlineBase64' }
$parts += @{ path = 'definition/pages/pages.json'; payload = (ToB64 $pagesMeta); payloadType = 'InlineBase64' }
$parts += @{ path = "definition/pages/$p1/page.json"; payload = (ToB64 (New-Page $p1 'Executive Overview')); payloadType = 'InlineBase64' }
$parts += @{ path = "definition/pages/$p2/page.json"; payload = (ToB64 (New-Page $p2 'Workforce')); payloadType = 'InlineBase64' }
$parts += @{ path = "definition/pages/$p3/page.json"; payload = (ToB64 (New-Page $p3 'Recruitment')); payloadType = 'InlineBase64' }
$parts += @{ path = "definition/pages/$p4/page.json"; payload = (ToB64 (New-Page $p4 'Compensation')); payloadType = 'InlineBase64' }
$parts += @{ path = "definition/pages/$p5/page.json"; payload = (ToB64 (New-Page $p5 'Time & Attendance')); payloadType = 'InlineBase64' }
$parts += @{ path = "definition/pages/$p6/page.json"; payload = (ToB64 (New-Page $p6 'AI Insights')); payloadType = 'InlineBase64' }
$parts += $p1Visuals
$parts += $p2Visuals
$parts += $p3Visuals
$parts += $p4Visuals
$parts += $p5Visuals
$parts += $p6Visuals
$parts += @{ path = '.platform'; payload = (ToB64 $platform); payloadType = 'InlineBase64' }

# --- PBIP output mode: write a Desktop-openable Power BI Project instead of deploying ---
if ($PbipOutDir) {
    $reportFolder = Join-Path $PbipOutDir "$PbipName.Report"
    if (Test-Path $reportFolder) { Remove-Item $reportFolder -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $reportFolder | Out-Null
    foreach ($p in $parts) {
        $dest = Join-Path $reportFolder ($p.path -replace '/', '\')
        $destDir = Split-Path $dest -Parent
        if (-not (Test-Path $destDir)) { New-Item -ItemType Directory -Force -Path $destDir | Out-Null }
        [IO.File]::WriteAllText($dest, [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($p.payload)), (New-Object Text.UTF8Encoding($false)))
    }
    $pbipFile = [ordered]@{
        '$schema'  = 'https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json'
        version    = '1.0'
        artifacts  = @(@{ report = @{ path = "$PbipName.Report" } })
        settings   = [ordered]@{ enableAutoRecovery = $true }
    }
    Set-Content -Path (Join-Path $PbipOutDir "$PbipName.pbip") -Value ($pbipFile | ConvertTo-Json -Depth 10) -Encoding UTF8
    Write-Host "PBIP project written to: $PbipOutDir" -ForegroundColor Green
    Write-Host "Open in Power BI Desktop: $(Join-Path $PbipOutDir "$PbipName.pbip")" -ForegroundColor Cyan
    return
}

function Wait-LRO($Response) {
    if ($Response.StatusCode -ne 202) { return }
    $uri = [string]$Response.Headers['Location']
    do { Start-Sleep -Seconds 5; $op = Invoke-RestMethod -Method Get -Uri $uri -Headers $headers } while ($op.status -notin @('Succeeded', 'Failed', 'Cancelled'))
    if ($op.status -ne 'Succeeded') { throw "Fabric operation $($op.status): $($op.error | ConvertTo-Json -Depth 8)" }
}

# Remove any existing report with this name first, to avoid a legacy/enhanced format clash.
$existing = (Invoke-RestMethod -Method Get -Uri "$apiBase/workspaces/$WorkspaceId/items?type=Report" -Headers $headers).value |
    Where-Object { $_.displayName -eq $ReportName }
foreach ($e in $existing) {
    Invoke-RestMethod -Method Delete -Uri "$apiBase/workspaces/$WorkspaceId/items/$($e.id)" -Headers $headers | Out-Null
    Write-Host "Deleted existing report $($e.id)" -ForegroundColor DarkYellow
}

$body = @{ displayName = $ReportName; type = 'Report'; definition = @{ parts = $parts } } | ConvertTo-Json -Depth 40
$resp = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers -Body $body -UseBasicParsing
Wait-LRO $resp
$reportId = ($resp.Content | ConvertFrom-Json).id
if (-not $reportId) {
    $reportId = ((Invoke-RestMethod -Method Get -Uri "$apiBase/workspaces/$WorkspaceId/items?type=Report" -Headers $headers).value | Where-Object { $_.displayName -eq $ReportName } | Select-Object -First 1).id
}
Write-Host "Created report '$ReportName' ($reportId)" -ForegroundColor Green
Write-Host "Open: https://app.fabric.microsoft.com/groups/$WorkspaceId/reports/$reportId" -ForegroundColor Cyan
