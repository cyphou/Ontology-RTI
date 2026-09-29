<#
.SYNOPSIS
  Shared helpers for spec-driven Power BI reports (report.spec.json).
.DESCRIPTION
  One spec drives both the HTML mockup (New-ReportMockup.ps1) and the PBIR report
  (Deploy-ReportFromSpec.ps1), so the approved mockup and the deployed report cannot drift.
  Dot-source this file. PowerShell 5.1 compatible.
#>

$script:ReportCanvas = @{ Width = 1280; Height = 720 }
$script:ReportVisualTypes = @('card', 'kpi', 'bar', 'column', 'line', 'donut', 'combo', 'funnel', 'waterfall', 'map', 'table', 'gauge', 'slicer', 'textbox', 'logo')
# Visuals that carry no measure: slicers list column values, textboxes are static text, logos are images.
$script:ReportNoMeasureTypes = @('slicer', 'textbox', 'logo')
# Visuals that are chrome, not content: excluded from density and KPI-anchor checks.
$script:ReportChromeTypes = @('slicer', 'textbox', 'logo')

# Visual selection rules, grounded in Microsoft report/dashboard design guidance. Editing the
# JSON changes the lint, so the mapping stays the single source of truth.
function Get-VisualMapping {
    if (-not $script:VisualMapping) {
        $path = Join-Path $PSScriptRoot 'visual-mapping.json'
        $script:VisualMapping = if (Test-Path $path) { Get-Content $path -Raw | ConvertFrom-Json } else { $null }
    }
    return $script:VisualMapping
}

function Get-VisualRule([string]$Name, $Fallback) {
    $rules = (Get-VisualMapping).rules
    if ($rules -and $null -ne $rules.$Name) { return $rules.$Name }
    return $Fallback
}

function Get-ApiToken([string]$Resource) {
    $token = $null
    try { $token = az account get-access-token --resource $Resource --query accessToken -o tsv 2>$null } catch { $token = $null }
    if (-not $token) {
        $t = (Get-AzAccessToken -ResourceUrl $Resource -WarningAction SilentlyContinue).Token
        if ($t -is [securestring]) { $t = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($t)) }
        $token = $t
    }
    if (-not $token) { throw "Could not acquire an access token for $Resource (run az login)." }
    return $token
}

# 'table[Field]' -> @{ Entity = 'table'; Property = 'Field' }
function ConvertTo-FieldRef([string]$Ref) {
    if ($Ref -notmatch '^\s*([^\[\]]+)\[([^\[\]]+)\]\s*$') { throw "Invalid field reference '$Ref' (expected table[Field])." }
    return @{ Entity = $Matches[1].Trim(); Property = $Matches[2].Trim() }
}

function Read-ReportSpec([string]$Path) {
    if (-not (Test-Path $Path)) { throw "Report spec not found: $Path" }
    $spec = Get-Content $Path -Raw | ConvertFrom-Json
    # A spec that names its domain inherits that domain's palette and logo, so brand colour
    # lives in one place instead of being copied into every spec.
    if (-not $spec.domain) {
        $folder = Split-Path (Split-Path $Path -Parent) -Leaf
        Add-Member -InputObject $spec -NotePropertyName domain -NotePropertyValue $folder -Force
    }
    Add-Member -InputObject $spec -NotePropertyName theme -NotePropertyValue (Resolve-SpecTheme $spec) -Force
    return $spec
}

# Brand metadata per ontology domain: one source shared by the theme, the generated logo and the mockup.
function Get-DomainBranding([string]$Domain) {
    if (-not $script:DomainBranding) {
        $path = Join-Path $PSScriptRoot 'domain-branding.json'
        $script:DomainBranding = if (Test-Path $path) { Get-Content $path -Raw | ConvertFrom-Json } else { $null }
    }
    if (-not $script:DomainBranding) { return $null }
    $d = $script:DomainBranding.domains.$Domain
    if (-not $d) { return $null }
    $merged = [ordered]@{}
    foreach ($p in $script:DomainBranding.defaults.PSObject.Properties) { if ($p.Name -ne '$comment') { $merged[$p.Name] = $p.Value } }
    foreach ($p in $d.PSObject.Properties) { $merged[$p.Name] = $p.Value }
    return [pscustomobject]$merged
}

# Spec theme wins where it is set; anything omitted falls back to the domain brand.
function Resolve-SpecTheme($Spec) {
    $brand = Get-DomainBranding $Spec.domain
    $theme = [ordered]@{}
    if ($brand) {
        foreach ($p in $brand.PSObject.Properties) { $theme[$p.Name] = $p.Value }
        $theme['name'] = "$($Spec.domain)Theme"
    }
    if ($Spec.theme) { foreach ($p in $Spec.theme.PSObject.Properties) { if ($null -ne $p.Value) { $theme[$p.Name] = $p.Value } } }
    if (-not $theme['name']) { $theme['name'] = 'ReportTheme' }
    return [pscustomobject]$theme
}

# Parses TMDL table files into measures (with formatString) and columns per table.
function Get-ModelFieldCatalog([string]$SemanticModelFolder) {
    $tablesDir = Join-Path $SemanticModelFolder 'definition\tables'
    $catalog = @{ Measures = @{}; Columns = @{} }
    foreach ($file in Get-ChildItem $tablesDir -Filter '*.tmdl') {
        $lines = Get-Content $file.FullName
        $table = $null; $current = $null
        foreach ($line in $lines) {
            if ($line -match "^table\s+'?([^']+?)'?\s*$") { $table = $Matches[1]; continue }
            if ($line -match "^\s*measure\s+'([^']+)'\s*=|^\s*measure\s+([^\s=']+)\s*=") {
                $name = if ($Matches[1]) { $Matches[1] } else { $Matches[2] }
                $current = "$table[$name]"; $catalog.Measures[$current] = @{ FormatString = '' }; continue
            }
            if ($line -match "^\s*column\s+'([^']+)'|^\s*column\s+([^\s=']+)") {
                $name = if ($Matches[1]) { $Matches[1] } else { $Matches[2] }
                $catalog.Columns["$table[$name]"] = $true; $current = $null; continue
            }
            if ($current -and $line -match '^\s*formatString:\s*(.+?)\s*$') { $catalog.Measures[$current].FormatString = $Matches[1] }
        }
    }
    return $catalog
}

# Returns a list of human-readable problems; empty list = valid spec.
function Test-ReportSpec($Spec, [string]$SemanticModelFolder) {
    $problems = New-Object System.Collections.Generic.List[string]
    $catalog = $null
    if ($SemanticModelFolder) { $catalog = Get-ModelFieldCatalog $SemanticModelFolder }
    if (-not $Spec.reportName) { $problems.Add('reportName is required.') }
    if (-not $Spec.pages -or @($Spec.pages).Count -eq 0) { $problems.Add('At least one page is required.'); return $problems }
    foreach ($page in $Spec.pages) {
        $boxes = @()
        foreach ($v in $page.visuals) {
            $label = "Page '$($page.name)' / visual '$($v.title)'"
            if ($script:ReportVisualTypes -notcontains $v.type) { $problems.Add("$label has unsupported type '$($v.type)'."); continue }
            if (($v.x + $v.w) -gt $script:ReportCanvas.Width -or ($v.y + $v.h) -gt $script:ReportCanvas.Height -or $v.x -lt 0 -or $v.y -lt 0) {
                $problems.Add("$label is outside the 1280x720 canvas.")
            }
            foreach ($b in $boxes) {
                # A textbox with a background is a band (e.g. page header) that other visuals may sit on.
                $isBand = ($b.type -eq 'textbox' -and $b.background) -or ($v.type -eq 'textbox' -and $v.background)
                if ($isBand) { continue }
                if ($v.x -lt ($b.x + $b.w) -and $b.x -lt ($v.x + $v.w) -and $v.y -lt ($b.y + $b.h) -and $b.y -lt ($v.y + $v.h)) { $problems.Add("$label overlaps '$($b.title)'.") }
            }
            $boxes += $v
            if (-not $v.measures -or @($v.measures).Count -eq 0) {
                if ($script:ReportNoMeasureTypes -notcontains $v.type) { $problems.Add("$label needs at least one measure.") }
            }
            $needsCategory = @('kpi', 'bar', 'column', 'line', 'donut', 'combo', 'funnel', 'waterfall', 'map', 'slicer') -contains $v.type
            if ($needsCategory -and -not $v.category) { $problems.Add("$label needs a category column.") }
            if ($v.type -eq 'textbox' -and -not $v.text) { $problems.Add("$label needs text.") }
            if ($v.type -eq 'logo') {
                if (-not $v.image) { $problems.Add("$label needs an image path.") }
                else {
                    $imgPath = if ([IO.Path]::IsPathRooted($v.image)) { $v.image } else { Join-Path (Split-Path $PSScriptRoot -Parent) $v.image }
                    if (-not (Test-Path $imgPath)) { $problems.Add("$label image not found: $($v.image) (run deploy/New-DomainLogos.ps1).") }
                }
            }
            if ($v.sort -and @('asc', 'desc') -notcontains $v.sort) { $problems.Add("$label sort must be 'asc' or 'desc'.") }
            if ($v.type -eq 'map' -and (-not $v.latitude -or -not $v.longitude)) { $problems.Add("$label needs latitude and longitude columns.") }
            if ($v.type -eq 'table' -and -not $v.columns) { $problems.Add("$label needs columns.") }
            if (-not $catalog) { continue }
            foreach ($m in @($v.measures | Where-Object { $_ }) + @($v.lineMeasures | Where-Object { $_ }) + @($v.goalMeasures | Where-Object { $_ })) {
                try { $null = ConvertTo-FieldRef $m } catch { $problems.Add("${label}: $($_.Exception.Message)"); continue }
                if (-not $catalog.Measures.ContainsKey($m)) { $problems.Add("$label references unknown measure $m.") }
            }
            foreach ($c in @($v.category, $v.latitude, $v.longitude) + @($v.columns) | Where-Object { $_ }) {
                try { $null = ConvertTo-FieldRef $c } catch { $problems.Add("${label}: $($_.Exception.Message)"); continue }
                if (-not $catalog.Columns.ContainsKey($c)) { $problems.Add("$label references unknown column $c.") }
            }
        }
    }
    return $problems
}

# Design lint (non-blocking): returns warnings about layout quality, never about correctness.
# Rules come from deploy/visual-mapping.json so the mapping and the lint cannot drift.
function Test-ReportLayout($Spec, [int]$Grid = 0, [int]$Margin = 0, [int]$MaxVisuals = 0) {
    if (-not $Grid) { $Grid = [int](Get-VisualRule 'grid' 4) }
    if (-not $Margin) { $Margin = [int](Get-VisualRule 'margin' 16) }
    if (-not $MaxVisuals) { $MaxVisuals = [int](Get-VisualRule 'max_content_visuals_per_page' 12) }
    $maxSlicers = [int](Get-VisualRule 'max_slicers_per_page' 3)
    $maxTypes = [int](Get-VisualRule 'max_distinct_visual_types_per_page' 6)
    $warnings = New-Object System.Collections.Generic.List[string]
    $genericTitle = '^\s*(Total\s+)?[\w\s%&+]+\s+by\s+[\w\s]+$'
    foreach ($page in $Spec.pages) {
        $content = @($page.visuals | Where-Object { -not ($_.type -eq 'textbox' -and $_.background) -and $_.type -notin $script:ReportChromeTypes })
        if ($content.Count -gt $MaxVisuals) { $warnings.Add("Page '$($page.name)' has $($content.Count) visuals; more than $MaxVisuals feels crowded.") }
        $offGrid = @($page.visuals | Where-Object { $v = $_; @($v.x, $v.y, $v.w, $v.h | Where-Object { $_ % $Grid -ne 0 }).Count -gt 0 })
        if ($offGrid.Count) { $warnings.Add("Page '$($page.name)': $($offGrid.Count) visual(s) off the ${Grid}px grid (e.g. '$(if ($offGrid[0].title) { $offGrid[0].title } else { $offGrid[0].type })' at $($offGrid[0].x),$($offGrid[0].y) $($offGrid[0].w)x$($offGrid[0].h)).") }
        $slicers = @($page.visuals | Where-Object { $_.type -eq 'slicer' })
        if ($slicers.Count -gt $maxSlicers) { $warnings.Add("Page '$($page.name)' has $($slicers.Count) slicers; keep at most $maxSlicers on canvas and move the rest to the filter pane.") }
        # "Avoid variety for the sake of variety": many chart types on one page read as a demo, not a report.
        $types = @($content | Select-Object -ExpandProperty type -Unique)
        if ($types.Count -gt $maxTypes) { $warnings.Add("Page '$($page.name)' mixes $($types.Count) visual types ($($types -join ', ')); variety for its own sake hurts readability.") }
        foreach ($v in $page.visuals) {
            $label = "Page '$($page.name)' / '$(if ($v.title) { $v.title } else { $v.type })'"
            $isBand = $v.type -eq 'textbox' -and $v.background
            if (-not $isBand -and $v.type -notin $script:ReportChromeTypes) {
                if ($v.x -gt 0 -and $v.x -lt $Margin) { $warnings.Add("$label is closer than ${Margin}px to the left edge.") }
                if (($v.x + $v.w) -lt $script:ReportCanvas.Width -and ($script:ReportCanvas.Width - $v.x - $v.w) -lt $Margin) { $warnings.Add("$label is closer than ${Margin}px to the right edge.") }
            }
            $minW = if ($v.type -in 'card', 'kpi', 'slicer', 'textbox', 'gauge', 'logo') { 120 } else { 240 }
            if ($v.type -ne 'logo' -and ($v.w -lt $minW -or ($v.type -notin 'slicer', 'textbox' -and $v.h -lt 100))) { $warnings.Add("$label is too small to read ($($v.w)x$($v.h)).") }
            if ($v.title -and $v.type -notin 'card', 'kpi', 'slicer', 'table' -and $v.title -match $genericTitle) {
                $warnings.Add("$label has a generic 'X by Y' title; state the finding instead (e.g. 'Pay per FTE is flat across job families').")
            }
            # Accessibility checklist: every non-decorative visual needs alt text.
            if ($v.type -notin 'textbox', 'logo' -and -not $v.altText) { $warnings.Add("$label has no altText; screen readers will announce only the title.") }
            # Visual-choice rules from the mapping.
            if ($v.type -eq 'gauge' -and $null -eq $v.target) { $warnings.Add("$label is a gauge without a target; a gauge only earns its space against a goal, otherwise use a card.") }
            if ($v.type -eq 'kpi' -and -not $v.goalMeasures) { $warnings.Add("$label is a KPI without goalMeasures; it renders as a plain number, so use a card.") }
            if ($v.type -eq 'combo' -and -not $v.lineMeasures) { $warnings.Add("$label is a combo without lineMeasures; the second axis is the only reason to use it.") }
            if ($v.type -in 'bar', 'column' -and -not $v.sort) { $warnings.Add("$label has no sort; sort by the measure to expose extremes, or by the axis for lookup.") }
        }
        # KPI/card row: same y and same height reads as one band; mixed heights look accidental.
        $kpis = @($page.visuals | Where-Object { $_.type -in 'card', 'kpi' } | Group-Object y | Where-Object { $_.Count -gt 1 })
        foreach ($row in $kpis) {
            if (@($row.Group | Select-Object -ExpandProperty h -Unique).Count -gt 1) { $warnings.Add("Page '$($page.name)': KPI row at y=$($row.Name) mixes heights.") }
        }
        if (@($page.visuals | Where-Object { $_.type -in 'card', 'kpi' }).Count -eq 0) { $warnings.Add("Page '$($page.name)' has no KPI/card row to anchor the reader.") }
    }
    return $warnings
}

# Data-aware lint: cardinality decides whether a visual choice holds up, and only the live
# query result knows it. Runs in the mockup, after each visual has been queried.
function Test-VisualFit($Visual, [int]$RowCount, $Rows) {
    $warnings = New-Object System.Collections.Generic.List[string]
    $label = "'$(if ($Visual.title) { $Visual.title } else { $Visual.type })'"
    $maxDonut = [int](Get-VisualRule 'max_donut_categories' 6)
    $minLine = [int](Get-VisualRule 'min_line_points' 4)
    $maxPeriods = [int](Get-VisualRule 'max_column_periods' 12)
    switch ($Visual.type) {
        'donut' {
            if ($RowCount -gt $maxDonut) { $warnings.Add("$label is a donut with $RowCount slices; Microsoft guidance caps part-to-whole charts at a handful. Use a sorted bar chart.") }
            if ($RowCount -le 2) { $warnings.Add("$label is a donut with only $RowCount slices; two numbers do not need a chart.") }
        }
        'line' {
            if ($RowCount -lt $minLine) { $warnings.Add("$label is a line with $RowCount points; below $minLine points a column chart is more honest.") }
        }
        'column' {
            if ($RowCount -gt $maxPeriods) { $warnings.Add("$label is a column chart with $RowCount categories; past $maxPeriods use a line (time) or a bar (names).") }
        }
        'waterfall' {
            $values = @($Rows | ForEach-Object { $_.m0 } | Where-Object { $null -ne $_ })
            if ($values.Count -and -not @($values | Where-Object { $_ -lt 0 }).Count) { $warnings.Add("$label is a waterfall but no value is negative; nothing subtracts, so a bar chart says the same thing.") }
        }
        'funnel' {
            $values = @($Rows | ForEach-Object { $_.m0 } | Where-Object { $null -ne $_ })
            $descending = $true
            for ($i = 1; $i -lt $values.Count; $i++) { if ($values[$i] -gt $values[$i - 1]) { $descending = $false } }
            if ($values.Count -gt 1 -and -not $descending) { $warnings.Add("$label is a funnel whose stages do not decrease; a funnel implies drop-off.") }
        }
        'table' {
            if ($RowCount -le 3) { $warnings.Add("$label is a table with $RowCount rows; that is a card or a bar chart.") }
        }
    }
    return $warnings
}

function Format-DaxColumn([string]$Ref) { $f = ConvertTo-FieldRef $Ref; return "'$($f.Entity)'[$($f.Property)]" }
function Format-DaxMeasure([string]$Ref) { $f = ConvertTo-FieldRef $Ref; return "[$($f.Property)]" }

# Builds the DAX query whose result shape the mockup renderer expects for each visual type.
# Returns $null for static visuals (textbox) that need no data.
function Get-VisualDax($Visual) {
    if ($Visual.type -in 'textbox', 'logo') { return $null }
    if ($Visual.type -eq 'slicer') { return "EVALUATE VALUES($(Format-DaxColumn $Visual.category)) ORDER BY $(Format-DaxColumn $Visual.category)" }
    $pairs = @(); $i = 0
    foreach ($m in $Visual.measures) { $pairs += "`"m$i`", $(Format-DaxMeasure $m)"; $i++ }
    $i = 0
    foreach ($m in @($Visual.lineMeasures | Where-Object { $_ })) { $pairs += "`"l$i`", $(Format-DaxMeasure $m)"; $i++ }
    $i = 0
    foreach ($m in @($Visual.goalMeasures | Where-Object { $_ })) { $pairs += "`"g$i`", $(Format-DaxMeasure $m)"; $i++ }
    $measureList = $pairs -join ', '
    switch ($Visual.type) {
        { $_ -in 'card', 'gauge' } { return "EVALUATE ROW($measureList)" }
        # The KPI indicator shows the latest trend point, so the mockup needs the series in order.
        'kpi' { return "EVALUATE SUMMARIZECOLUMNS($(Format-DaxColumn $Visual.category), $measureList) ORDER BY $(Format-DaxColumn $Visual.category)" }
        'map' {
            $geo = "`"lat`", AVERAGE($(Format-DaxColumn $Visual.latitude)), `"lon`", AVERAGE($(Format-DaxColumn $Visual.longitude))"
            return "EVALUATE SUMMARIZECOLUMNS($(Format-DaxColumn $Visual.category), $geo, $measureList)"
        }
        'table' {
            $cols = (@($Visual.columns) | ForEach-Object { Format-DaxColumn $_ }) -join ', '
            $top = if ($Visual.maxRows) { [int]$Visual.maxRows } else { 50 }
            return "EVALUATE TOPN($top, SUMMARIZECOLUMNS($cols, $measureList), [m0], DESC) ORDER BY [m0] DESC"
        }
        default {
            $order = switch ($Visual.sort) { 'desc' { ' ORDER BY [m0] DESC' } 'asc' { " ORDER BY $(Format-DaxColumn $Visual.category)" } default { '' } }
            return "EVALUATE SUMMARIZECOLUMNS($(Format-DaxColumn $Visual.category), $measureList)$order"
        }
    }
}

function Invoke-ModelDax([string]$WorkspaceId, [string]$SemanticModelId, [string]$Dax, [string]$Token) {
    $body = @{ queries = @(@{ query = $Dax }); serializerSettings = @{ includeNulls = $true } } | ConvertTo-Json -Depth 5
    $uri = "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/datasets/$SemanticModelId/executeQueries"
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            $r = Invoke-RestMethod -Method Post -Uri $uri -Headers @{ Authorization = "Bearer $Token" } -ContentType 'application/json' -Body $body
            return @{ Ok = $true; Rows = @($r.results[0].tables[0].rows) }
        } catch {
            # No HTTP response = transport failure (SSL/DNS/reset): retry. HTTP errors are real DAX/service errors.
            if (-not $_.Exception.Response -and $attempt -lt 3) { Start-Sleep -Seconds (2 * $attempt); continue }
            $detail = $_.Exception.Message
            try { $detail = (($_.ErrorDetails.Message | ConvertFrom-Json).error.'pbi.error'.details | ForEach-Object { $_.detail.value }) -join ' | ' } catch { }
            return @{ Ok = $false; Error = $detail }
        }
    }
}
