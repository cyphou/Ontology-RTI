<#
.SYNOPSIS
  Shared helpers for spec-driven Power BI reports (report.spec.json).
.DESCRIPTION
  One spec drives both the HTML mockup (New-ReportMockup.ps1) and the PBIR report
  (Deploy-ReportFromSpec.ps1), so the approved mockup and the deployed report cannot drift.
  Dot-source this file. PowerShell 5.1 compatible.
#>

$script:ReportCanvas = @{ Width = 1280; Height = 720 }
$script:ReportVisualTypes = @('card', 'kpi', 'bar', 'column', 'line', 'donut', 'combo', 'funnel', 'waterfall', 'map', 'table', 'gauge', 'slicer', 'textbox')
# Visuals that carry no measure: slicers list column values, textboxes are static text.
$script:ReportNoMeasureTypes = @('slicer', 'textbox')

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
    return (Get-Content $Path -Raw | ConvertFrom-Json)
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
function Test-ReportLayout($Spec, [int]$Grid = 4, [int]$Margin = 16, [int]$MaxVisuals = 12) {
    $warnings = New-Object System.Collections.Generic.List[string]
    $genericTitle = '^\s*(Total\s+)?[\w\s%&+]+\s+by\s+[\w\s]+$'
    foreach ($page in $Spec.pages) {
        $content = @($page.visuals | Where-Object { -not ($_.type -eq 'textbox' -and $_.background) -and $_.type -ne 'slicer' })
        if ($content.Count -gt $MaxVisuals) { $warnings.Add("Page '$($page.name)' has $($content.Count) visuals; more than $MaxVisuals feels crowded.") }
        $offGrid = @($page.visuals | Where-Object { $v = $_; @($v.x, $v.y, $v.w, $v.h | Where-Object { $_ % $Grid -ne 0 }).Count -gt 0 })
        if ($offGrid.Count) { $warnings.Add("Page '$($page.name)': $($offGrid.Count) visual(s) off the ${Grid}px grid (e.g. '$(if ($offGrid[0].title) { $offGrid[0].title } else { $offGrid[0].type })' at $($offGrid[0].x),$($offGrid[0].y) $($offGrid[0].w)x$($offGrid[0].h)).") }
        foreach ($v in $page.visuals) {
            $label = "Page '$($page.name)' / '$(if ($v.title) { $v.title } else { $v.type })'"
            $isBand = $v.type -eq 'textbox' -and $v.background
            if (-not $isBand -and $v.type -ne 'slicer') {
                if ($v.x -gt 0 -and $v.x -lt $Margin) { $warnings.Add("$label is closer than ${Margin}px to the left edge.") }
                if (($v.x + $v.w) -lt $script:ReportCanvas.Width -and ($script:ReportCanvas.Width - $v.x - $v.w) -lt $Margin) { $warnings.Add("$label is closer than ${Margin}px to the right edge.") }
            }
            $minW = if ($v.type -in 'card', 'kpi', 'slicer', 'textbox', 'gauge') { 120 } else { 240 }
            if ($v.w -lt $minW -or ($v.type -notin 'slicer', 'textbox' -and $v.h -lt 100)) { $warnings.Add("$label is too small to read ($($v.w)x$($v.h)).") }
            if ($v.title -and $v.type -notin 'card', 'kpi', 'slicer', 'table' -and $v.title -match $genericTitle) {
                $warnings.Add("$label has a generic 'X by Y' title; state the finding instead (e.g. 'Pay per FTE is flat across job families').")
            }
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

function Format-DaxColumn([string]$Ref) { $f = ConvertTo-FieldRef $Ref; return "'$($f.Entity)'[$($f.Property)]" }
function Format-DaxMeasure([string]$Ref) { $f = ConvertTo-FieldRef $Ref; return "[$($f.Property)]" }

# Builds the DAX query whose result shape the mockup renderer expects for each visual type.
# Returns $null for static visuals (textbox) that need no data.
function Get-VisualDax($Visual) {
    if ($Visual.type -eq 'textbox') { return $null }
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
