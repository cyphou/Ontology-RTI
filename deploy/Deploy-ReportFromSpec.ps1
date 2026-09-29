<#
.SYNOPSIS
  Builds and deploys a PBIR report from report.spec.json, after a mandatory HTML mockup gate.
.DESCRIPTION
  Chain: validate spec -> New-ReportMockup (live DAX per visual) -> PBIR build -> deploy.
  The report is NOT created if any visual fails in the mockup (override with -Force).
  -MockupOnly stops after the mockup so the spec can be iterated (e.g. by the Report Mockup agent).
  -PbipOutDir writes a Desktop-openable PBIP instead of deploying.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$SpecPath,
    [Parameter(Mandatory = $true)][string]$WorkspaceId,
    [Parameter(Mandatory = $true)][string]$SemanticModelId,
    [string]$SemanticModelFolder = '',
    [string]$PbipOutDir = '',
    [string]$PbipName = '',
    [switch]$MockupOnly,
    [switch]$SkipMockup,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'ReportSpec.ps1')

$spec = Read-ReportSpec $SpecPath
if (-not $SemanticModelFolder) { $SemanticModelFolder = Join-Path (Split-Path $SpecPath -Parent) 'SemanticModel' }
if (-not $PbipName) { $PbipName = ($spec.reportName -replace '[^A-Za-z0-9]', '') }

# --- Gate: mockup on live data before any report is created ---
if (-not $SkipMockup) {
    Write-Host "Generating report mockup (validation gate)..." -ForegroundColor Cyan
    $mockup = & (Join-Path $PSScriptRoot 'New-ReportMockup.ps1') -SpecPath $SpecPath -WorkspaceId $WorkspaceId -SemanticModelId $SemanticModelId -SemanticModelFolder $SemanticModelFolder
    if ($MockupOnly) { return $mockup }
    if (-not $mockup.Ok) {
        if (-not $Force) { throw "Mockup gate failed ($(@($mockup.Errors).Count) issue(s)); fix report.spec.json or rerun with -Force. First: $(@($mockup.Errors)[0])" }
        Write-Warning "Mockup gate failed but -Force was set; continuing."
    }
} else {
    $problems = @(Test-ReportSpec $spec $SemanticModelFolder)
    if ($problems.Count -gt 0 -and -not $Force) { throw "Invalid report spec: $($problems -join '; ')" }
}

$apiBase = 'https://api.fabric.microsoft.com/v1'
$token = Get-ApiToken 'https://api.fabric.microsoft.com'
$headers = @{ Authorization = "Bearer $token"; 'Content-Type' = 'application/json' }
$workspaceName = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId" -Headers $headers).displayName
$modelName = ((Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items?type=SemanticModel" -Headers $headers).value |
    Where-Object { $_.id -eq $SemanticModelId } | Select-Object -First 1).displayName
if (-not $modelName) { throw "Semantic model $SemanticModelId not found in workspace $WorkspaceId." }

$sbReport = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.1.0/schema.json'
$sbPage   = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json'
$sbVisual = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.6.0/schema.json'

function ToB64([object]$Obj) {
    $json = if ($Obj -is [string]) { $Obj } else { $Obj | ConvertTo-Json -Depth 40 }
    [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($json))
}
function NewName { [guid]::NewGuid().ToString('N').Substring(0, 20) }
function Lit([string]$v) { @{ expr = @{ Literal = @{ Value = $v } } } }
function Num([double]$v) { Lit ("{0}D" -f $v.ToString([Globalization.CultureInfo]::InvariantCulture)) }

function Proj([string]$kind, [string]$ref) {
    $f = ConvertTo-FieldRef $ref
    @{ field = @{ $kind = @{ Expression = @{ SourceRef = @{ Entity = $f.Entity } }; Property = $f.Property } }; queryRef = "$($f.Entity).$($f.Property)"; nativeQueryRef = $f.Property }
}
function M([string]$ref) { Proj 'Measure' $ref }
function C([string]$ref) { Proj 'Column' $ref }
# Function 1 = Average; Azure Maps expects aggregated coordinate columns.
function Avg([string]$ref) {
    $f = ConvertTo-FieldRef $ref
    $col = @{ Column = @{ Expression = @{ SourceRef = @{ Entity = $f.Entity } }; Property = $f.Property } }
    @{ field = @{ Aggregation = @{ Expression = $col; Function = 1 } }; queryRef = "Average($($f.Entity).$($f.Property))"; nativeQueryRef = "Average of $($f.Property)" }
}

function New-PbirVisual($v) {
    $m0 = if ($v.measures) { M $v.measures[0] } else { $null }
    $roles = @{}; $objects = $null; $sortField = $null
    switch ($v.type) {
        'logo' {
            # The image ships as a RegisteredResources item, so the report needs no public URL.
            $item = Split-Path $v.image -Leaf
            $visual = @{
                visualType = 'image'
                objects    = @{ general = @(@{ properties = @{ imageUrl = @{ expr = @{ ResourcePackageItem = [ordered]@{ PackageName = 'RegisteredResources'; PackageType = 1; ItemName = $item } } } } }) }
                drillFilterOtherVisuals = $false
            }
            $visual.visualContainerObjects = New-ContainerObjects $v
            return $visual
        }
        'textbox' {
            $run = [ordered]@{ value = [string]$v.text; textStyle = [ordered]@{ fontFamily = 'Segoe UI Semibold'; fontSize = "$(if ($v.fontSize) { $v.fontSize } else { 14 })pt"; color = $(if ($v.color) { $v.color } else { $t.foreground }) } }
            $para = [ordered]@{ textRuns = @($run) }
            if ($v.subtext) {
                $sub = [ordered]@{ value = [string]$v.subtext; textStyle = [ordered]@{ fontFamily = 'Segoe UI'; fontSize = "$(if ($v.subFontSize) { $v.subFontSize } else { 10 })pt"; color = $(if ($v.subColor) { $v.subColor } else { $t.mutedText }) } }
                $visual = @{ visualType = 'textbox'; objects = @{ general = @(@{ properties = @{ paragraphs = @($para, [ordered]@{ textRuns = @($sub) }) } }) }; drillFilterOtherVisuals = $false }
            } else {
                $visual = @{ visualType = 'textbox'; objects = @{ general = @(@{ properties = @{ paragraphs = @($para) } }) }; drillFilterOtherVisuals = $false }
            }
            $visual.visualContainerObjects = New-ContainerObjects $v
            return $visual
        }
        'slicer' {
            $type = 'slicer'; $roles.Values = @(C $v.category)
            # The container title already names the slicer; hide the raw field-name header.
            $objects = @{ data = @(@{ properties = @{ mode = Lit "'Dropdown'" } }); header = @(@{ properties = @{ show = Lit 'false' } }) }
        }
        'kpi' {
            $type = 'kpi'; $roles.Indicator = @($m0); $roles.TrendLine = @(C $v.category)
            if ($v.goalMeasures) { $roles.Goal = @(M $v.goalMeasures[0]) }
            $objects = @{
                goals     = @(@{ properties = @{ show = Lit 'true'; showGoal = Lit 'true'; showDistance = Lit 'true'; distanceLabel = Lit "'Percent'" } })
                # Without a goal the trend area is grey and auto-scaled, which turns flat data into noise.
                trendline = @(@{ properties = @{ show = Lit $(if ($v.goalMeasures) { 'true' } else { 'false' }); transparency = Lit '92D' } })
                status    = @(@{ properties = @{
                            direction    = Lit $(if ($v.lowerIsBetter) { "'Decreasing'" } else { "'Increasing'" })
                            goodColor    = @{ solid = @{ color = Lit "'$($t.good)'" } }
                            neutralColor = @{ solid = @{ color = Lit "'$($t.neutral)'" } }
                            badColor     = @{ solid = @{ color = Lit "'$($t.bad)'" } }
                        } })
            }
        }
        'funnel' { $type = 'funnel'; $roles.Category = @(C $v.category); $roles.Y = @($m0) }
        'waterfall' { $type = 'waterfallChart'; $roles.Category = @(C $v.category); $roles.Y = @($m0) }
        'card' {
            $type = 'card'; $roles.Values = @($m0)
            if ($null -ne $v.precision) { $objects = @{ labels = @(@{ properties = @{ labelPrecision = Lit ("{0}L" -f [int]$v.precision) } }) } }
        }
        'bar' { $type = 'clusteredBarChart'; $roles.Category = @(C $v.category); $roles.Y = @($v.measures | ForEach-Object { M $_ }) }
        'column' { $type = 'clusteredColumnChart'; $roles.Category = @(C $v.category); $roles.Y = @($v.measures | ForEach-Object { M $_ }) }
        'line' {
            $type = 'lineChart'; $roles.Category = @(C $v.category); $roles.Y = @($v.measures | ForEach-Object { M $_ })
            # Power BI auto-scales line axes, which exaggerates small moves; the mockup is zero-based.
            $objects = @{ valueAxis = @(@{ properties = @{ start = Lit '0D' } }) }
        }
        'donut' { $type = 'donutChart'; $roles.Category = @(C $v.category); $roles.Y = @($m0) }
        'combo' {
            $type = 'lineClusteredColumnComboChart'; $roles.Category = @(C $v.category)
            $roles.Y = @($v.measures | ForEach-Object { M $_ }); $roles.Y2 = @($v.lineMeasures | ForEach-Object { M $_ })
        }
        'map' {
            $type = 'azureMap'
            $roles.Category = @(C $v.category); $roles.Y = @(Avg $v.latitude); $roles.X = @(Avg $v.longitude); $roles.Size = @($m0)
            $objects = @{
                mapControls = @(@{ properties = @{ defaultStyle = Lit "'grayscale_light'"; showStylePicker = Lit 'false' } })
                bubbleLayer = @(@{ properties = @{ show = Lit 'true'; minBubbleRadius = Lit '8L'; maxRadius = Lit '28L' } })
            }
        }
        'table' {
            $type = 'tableEx'
            $roles.Values = @(@($v.columns | ForEach-Object { C $_ }) + @($v.measures | ForEach-Object { M $_ }))
            if ($v.fontSize) {
                $fs = Lit ("{0}D" -f [int]$v.fontSize)
                $objects = @{ values = @(@{ properties = @{ fontSize = $fs } }); columnHeaders = @(@{ properties = @{ fontSize = $fs } }); total = @(@{ properties = @{ fontSize = $fs } }) }
            }
        }
        'gauge' {
            $type = 'gauge'; $roles.Y = @($m0)
            $axis = @{}
            if ($null -ne $v.min) { $axis.min = Num $v.min }
            if ($null -ne $v.max) { $axis.max = Num $v.max }
            if ($null -ne $v.target) { $axis.target = Num $v.target }
            if ($axis.Count) { $objects = @{ axis = @(@{ properties = $axis }) } }
        }
    }
    # The container title already says what the chart shows, so axis titles only repeat the
    # field names. Microsoft guidance: remove unnecessary labels.
    if ($type -in 'clusteredBarChart', 'clusteredColumnChart', 'lineChart', 'lineClusteredColumnComboChart', 'funnel', 'waterfallChart') {
        if (-not $objects) { $objects = @{} }
        $catProps = @{ showAxisTitle = Lit 'false' }
        # Long category names on a dense axis rotate into unreadable stubs; the trend shape is the message.
        if ($v.hideCategoryLabels) { $catProps.show = Lit 'false' }
        $objects.categoryAxis = @(@{ properties = $catProps })
        if ($objects.valueAxis) { $objects.valueAxis[0].properties.showAxisTitle = Lit 'false' }
        else { $objects.valueAxis = @(@{ properties = @{ showAxisTitle = Lit 'false' } }) }
    }
    $sortDirection = 'Descending'
    if ($v.sort -eq 'desc' -and $m0) { $sortField = $m0.field }
    elseif ($v.sort -eq 'asc' -and $v.category) { $sortField = (C $v.category).field; $sortDirection = 'Ascending' }
    $state = @{}; foreach ($k in $roles.Keys) { $state[$k] = @{ projections = $roles[$k] } }
    $query = @{ queryState = $state }
    if ($sortField) { $query.sortDefinition = @{ sort = @(@{ field = $sortField; direction = $sortDirection }); isDefaultSort = $true } }
    $visual = @{ visualType = $type; query = $query; drillFilterOtherVisuals = $true }
    if ($objects) { $visual.objects = $objects }
    $visual.visualContainerObjects = New-ContainerObjects $v
    return $visual
}

# Title plus optional container background (used for header bands) and hidden border/shadow.
function New-ContainerObjects($v) {
    $o = @{}
    if ($v.title) { $o.title = @(@{ properties = @{ show = Lit 'true'; text = Lit "'$($v.title.Replace("'", "''"))'" } }) }
    else { $o.title = @(@{ properties = @{ show = Lit 'false' } }) }
    # Accessibility: alt text is what a screen reader announces for the visual.
    if ($v.altText) { $o.general = @(@{ properties = @{ altText = Lit "'$($v.altText.Replace("'", "''"))'" } }) }
    if ($v.background) {
        $o.background = @(@{ properties = @{ show = Lit 'true'; color = @{ solid = @{ color = Lit "'$($v.background)'" } }; transparency = Lit '0D' } })
    }
    if ($v.plain) {
        $o.border = @(@{ properties = @{ show = Lit 'false' } })
        $o.dropShadow = @(@{ properties = @{ show = Lit 'false' } })
        if (-not $v.background) { $o.background = @(@{ properties = @{ show = Lit 'false' } }) }
    }
    return $o
}

$t = $spec.theme
$themeName = if ($t.name) { $t.name } else { 'ReportTheme' }
$themeObj = [ordered]@{
    name        = $themeName
    dataColors  = @($t.dataColors)
    background  = $t.visualBackground; foreground = $t.foreground; tableAccent = $t.accent
    good        = $t.good; neutral = $t.neutral; bad = $t.bad
    textClasses = [ordered]@{
        callout = @{ fontFace = 'Segoe UI Semibold'; fontSize = 28; color = $t.foreground }
        title   = @{ fontFace = 'Segoe UI Semibold'; fontSize = 12; color = $t.mutedText }
        header  = @{ fontFace = 'Segoe UI Semibold'; fontSize = 10; color = $t.mutedText }
        label   = @{ fontFace = 'Segoe UI'; fontSize = 10; color = $t.mutedText }
    }
    visualStyles = [ordered]@{
        '*'     = @{ '*' = [ordered]@{
                background = @(@{ show = $true; color = @{ solid = @{ color = $t.visualBackground } }; transparency = 0 })
                border     = @(@{ show = $true; color = @{ solid = @{ color = $t.border } }; radius = 16 })
                dropShadow = @(@{ show = $true; preset = 'BottomRight' })
                title      = @(@{ show = $true; fontColor = @{ solid = @{ color = $t.mutedText } }; fontSize = 12; bold = $true; fontFamily = 'Segoe UI Semibold'; alignment = 'left' })
            } }
        page    = @{ '*' = @{
                background = @(@{ color = @{ solid = @{ color = $t.pageBackground } }; transparency = 0 })
                outspace   = @(@{ color = @{ solid = @{ color = $t.outspace } }; transparency = 0 })
            } }
        card    = @{ '*' = @{
                labels         = @(@{ color = @{ solid = @{ color = $t.foreground } }; fontSize = 28; fontFamily = 'Segoe UI Semibold' })
                categoryLabels = @(@{ show = $false })
            } }
        tableEx = @{ '*' = @{
                columnHeaders = @(@{ fontColor = @{ solid = @{ color = '#FFFFFF' } }; backColor = @{ solid = @{ color = $t.accent } }; bold = $true })
            } }
    }
}

$rvi = [ordered]@{ visual = '2.6.0'; report = '3.1.0'; page = '2.3.0' }
# Logos referenced by the spec travel inside the report as registered resources.
$logoItems = @()
$logoParts = @()
foreach ($img in @($spec.pages.visuals | Where-Object { $_.type -eq 'logo' -and $_.image } | Select-Object -ExpandProperty image -Unique)) {
    $path = if ([IO.Path]::IsPathRooted($img)) { $img } else { Join-Path (Split-Path $PSScriptRoot -Parent) $img }
    if (-not (Test-Path $path)) { throw "Logo image not found: $path" }
    $item = Split-Path $path -Leaf
    $logoItems += @{ name = $item; path = $item; type = 'Image' }
    $logoParts += @{ path = "StaticResources/RegisteredResources/$item"; payload = [Convert]::ToBase64String([IO.File]::ReadAllBytes($path)); payloadType = 'InlineBase64' }
}
$parts = @()
$pageNames = @()
foreach ($page in $spec.pages) {
    $pageName = (NewName).Substring(0, 16); $pageNames += $pageName
    $pageObj = [ordered]@{ '$schema' = $sbPage; name = $pageName; displayName = $page.name; displayOption = 'FitToPage'; height = 720; width = 1280 }
    $parts += @{ path = "definition/pages/$pageName/page.json"; payload = (ToB64 $pageObj); payloadType = 'InlineBase64' }
    $zIndex = 0
    foreach ($v in $page.visuals) {
        $name = NewName
        # Spec order is paint order: header bands first, visuals layered on top.
        $container = [ordered]@{
            '$schema' = $sbVisual; name = $name
            position  = [ordered]@{ x = [int]$v.x; y = [int]$v.y; z = $zIndex * 1000; width = [int]$v.w; height = [int]$v.h; tabOrder = $(if ($v.type -eq 'logo') { -1 } else { $zIndex * 1000 }) }
            visual    = (New-PbirVisual $v)
        }
        $zIndex++
        $parts += @{ path = "definition/pages/$pageName/visuals/$name/visual.json"; payload = (ToB64 $container); payloadType = 'InlineBase64' }
    }
}

$connStr = "Data Source=`"powerbi://api.powerbi.com/v1.0/myorg/$workspaceName`";initial catalog=$modelName;access mode=readonly;integrated security=ClaimsToken;semanticmodelid=$SemanticModelId"
$pbir = [ordered]@{
    '$schema'        = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json'
    version          = '4.0'
    datasetReference = [ordered]@{ byConnection = [ordered]@{ connectionString = $connStr } }
}
# customTheme.name must include ".json" and equal the RegisteredResources item, otherwise the theme is ignored.
$report = [ordered]@{
    '$schema'        = $sbReport
    themeCollection  = [ordered]@{
        baseTheme   = [ordered]@{ name = 'CY26SU02'; reportVersionAtImport = $rvi; type = 'SharedResources' }
        customTheme = [ordered]@{ name = "$themeName.json"; reportVersionAtImport = $rvi; type = 'RegisteredResources' }
    }
    resourcePackages = @(
        @{ name = 'SharedResources'; type = 'SharedResources'; items = @(@{ name = 'CY26SU02'; path = 'BaseThemes/CY26SU02.json'; type = 'BaseTheme' }) },
        @{ name = 'RegisteredResources'; type = 'RegisteredResources'; items = @(@{ name = "$themeName.json"; path = "$themeName.json"; type = 'CustomTheme' }) + $logoItems }
    )
    settings         = [ordered]@{ useStylableVisualContainerHeader = $true; defaultDrillFilterOtherVisuals = $true }
}
$pagesMeta = [ordered]@{
    '$schema' = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json'
    pageOrder = $pageNames; activePageName = $pageNames[0]
}
$platform = [ordered]@{
    '$schema' = 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json'
    metadata  = [ordered]@{ type = 'Report'; displayName = $spec.reportName }
    config    = [ordered]@{ version = '2.0'; logicalId = [guid]::NewGuid().ToString() }
}
$versionMeta = [ordered]@{ '$schema' = 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json'; version = '2.0.0' }

$parts = @(
    @{ path = 'definition.pbir'; payload = (ToB64 $pbir); payloadType = 'InlineBase64' },
    @{ path = 'definition/version.json'; payload = (ToB64 $versionMeta); payloadType = 'InlineBase64' },
    @{ path = 'definition/report.json'; payload = (ToB64 $report); payloadType = 'InlineBase64' },
    @{ path = "StaticResources/RegisteredResources/$themeName.json"; payload = (ToB64 $themeObj); payloadType = 'InlineBase64' },
    @{ path = 'definition/pages/pages.json'; payload = (ToB64 $pagesMeta); payloadType = 'InlineBase64' },
    @{ path = '.platform'; payload = (ToB64 $platform); payloadType = 'InlineBase64' }
) + $logoParts + $parts

if ($PbipOutDir) {
    $reportFolder = Join-Path $PbipOutDir "$PbipName.Report"
    if (Test-Path $reportFolder) { Remove-Item $reportFolder -Recurse -Force }
    foreach ($p in $parts) {
        $dest = Join-Path $reportFolder ($p.path -replace '/', '\')
        New-Item -ItemType Directory -Force -Path (Split-Path $dest -Parent) | Out-Null
        $bytes = [Convert]::FromBase64String($p.payload)
        # Images are binary; decoding them as UTF-8 text would corrupt the file.
        if ($p.path -match '\.(png|jpg|jpeg|gif|bmp)$') { [IO.File]::WriteAllBytes($dest, $bytes) }
        else { [IO.File]::WriteAllText($dest, [Text.Encoding]::UTF8.GetString($bytes), (New-Object Text.UTF8Encoding($false))) }
    }
    $pbipFile = [ordered]@{
        '$schema' = 'https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json'
        version   = '1.0'; artifacts = @(@{ report = @{ path = "$PbipName.Report" } }); settings = [ordered]@{ enableAutoRecovery = $true }
    }
    Set-Content -Path (Join-Path $PbipOutDir "$PbipName.pbip") -Value ($pbipFile | ConvertTo-Json -Depth 10) -Encoding UTF8
    Write-Host "PBIP project written to: $(Join-Path $PbipOutDir "$PbipName.pbip")" -ForegroundColor Green
    return
}

# Create under a temporary name first so a rejected definition never destroys the existing report.
$tempName = "$($spec.reportName) (deploying)"
$previous = @((Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items?type=Report" -Headers $headers).value |
    Where-Object { $_.displayName -in @($spec.reportName, $tempName) })

$body = @{ displayName = $tempName; type = 'Report'; definition = @{ parts = $parts } } | ConvertTo-Json -Depth 40
$resp = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers -Body $body -UseBasicParsing
if ($resp.StatusCode -eq 202) {
    $loc = [string]$resp.Headers['Location']
    do { Start-Sleep -Seconds 5; $op = Invoke-RestMethod -Uri $loc -Headers $headers } while ($op.status -notin @('Succeeded', 'Failed', 'Cancelled'))
    if ($op.status -ne 'Succeeded') { throw "Report creation $($op.status): $($op.error | ConvertTo-Json -Depth 8)" }
}
$reportId = ((Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items?type=Report" -Headers $headers).value |
    Where-Object { $_.displayName -eq $tempName -and $_.id -notin $previous.id } | Select-Object -First 1).id
if (-not $reportId) { throw 'Report was created but could not be located.' }

# Replace rather than update, to avoid a legacy/enhanced format clash on existing reports.
foreach ($old in $previous) {
    Invoke-RestMethod -Method Delete -Uri "$apiBase/workspaces/$WorkspaceId/items/$($old.id)" -Headers $headers | Out-Null
    Write-Host "Replaced previous report $($old.id)" -ForegroundColor DarkYellow
}
Invoke-RestMethod -Method Patch -Uri "$apiBase/workspaces/$WorkspaceId/items/$reportId" -Headers $headers -Body (@{ displayName = $spec.reportName } | ConvertTo-Json) | Out-Null
Write-Host "Created report '$($spec.reportName)' ($reportId)" -ForegroundColor Green
Write-Host "Open: https://app.fabric.microsoft.com/groups/$WorkspaceId/reports/$reportId" -ForegroundColor Cyan
return [pscustomobject]@{ ReportId = $reportId; MockupFile = $mockup.OutFile }
