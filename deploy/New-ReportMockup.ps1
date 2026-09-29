<#
.SYNOPSIS
  Generates the HTML mockup of a spec-driven report from live semantic-model data.
.DESCRIPTION
  Chain step that runs BEFORE report creation: validates report.spec.json against the TMDL,
  executes one DAX query per visual, and renders a 1280x720-per-page HTML preview using the
  report theme. Returns an object with Ok = $false when any visual cannot be built, which
  Deploy-ReportFromSpec.ps1 uses as a gate.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$SpecPath,
    [Parameter(Mandatory = $true)][string]$WorkspaceId,
    [Parameter(Mandatory = $true)][string]$SemanticModelId,
    [string]$SemanticModelFolder = '',
    [string]$OutFile = ''
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'ReportSpec.ps1')

$spec = Read-ReportSpec $SpecPath
if (-not $SemanticModelFolder) { $SemanticModelFolder = Join-Path (Split-Path $SpecPath -Parent) 'SemanticModel' }
if (-not $OutFile) {
    $domain = Split-Path (Split-Path $SpecPath -Parent) -Leaf
    $OutFile = Join-Path (Split-Path $PSScriptRoot -Parent) "artifacts\$domain-report-mockup.html"
}

$problems = @(Test-ReportSpec $spec $SemanticModelFolder)
if ($problems.Count -gt 0) {
    $problems | ForEach-Object { Write-Host "  [SPEC] $_" -ForegroundColor Red }
    return [pscustomobject]@{ Ok = $false; OutFile = $null; Errors = $problems }
}

$catalog = Get-ModelFieldCatalog $SemanticModelFolder
$layoutWarnings = @(Test-ReportLayout $spec)
$layoutWarnings | ForEach-Object { Write-Host "  [LAYOUT] $_" -ForegroundColor Yellow }
$token = Get-ApiToken 'https://analysis.windows.net/powerbi/api'
$errors = New-Object System.Collections.Generic.List[string]
$visualData = @{}
$formats = @{}
$pageIndex = 0
foreach ($page in $spec.pages) {
    $visualIndex = 0
    foreach ($v in $page.visuals) {
        $key = "p$pageIndex-v$visualIndex"
        foreach ($m in @($v.measures | Where-Object { $_ }) + @($v.lineMeasures | Where-Object { $_ }) + @($v.goalMeasures | Where-Object { $_ })) { $formats[$m] = $catalog.Measures[$m].FormatString }
        $dax = Get-VisualDax $v
        if (-not $dax) { $visualData[$key] = @{ rows = @() }; $visualIndex++; continue }
        $result = Invoke-ModelDax $WorkspaceId $SemanticModelId $dax $token
        if ($result.Ok) {
            # Strip DAX column decoration: "[m0]" -> "m0"; "table[Col]" stays as the spec reference.
            $rows = @(foreach ($row in $result.Rows) {
                    $o = [ordered]@{}
                    foreach ($p in $row.PSObject.Properties) { $name = if ($p.Name -match '^\[(.+)\]$') { $Matches[1] } else { $p.Name }; $o[$name] = $p.Value }
                    $o
                })
            $visualData[$key] = @{ rows = $rows }
            if ($rows.Count -eq 0) { $errors.Add("Page '$($page.name)' / '$($v.title)': query returned no rows.") }
            Write-Host "  [OK]   $($page.name) / $($v.title) ($($rows.Count) rows)" -ForegroundColor Green
        } else {
            $visualData[$key] = @{ error = $result.Error }
            $errors.Add("Page '$($page.name)' / '$($v.title)': $($result.Error)")
            Write-Host "  [FAIL] $($page.name) / $($v.title): $($result.Error)" -ForegroundColor Red
        }
        $visualIndex++
    }
    $pageIndex++
}

# JSON is embedded in a <script>; neutralise "</" so data can never close the tag.
$toJs = { param($o) $json = ConvertTo-Json -InputObject $o -Depth 20 -Compress; if (-not $json) { $json = '[]' }; $json.Replace('</', '<\/') }
$template = Get-Content (Join-Path $PSScriptRoot 'report-mockup.template.html') -Raw
$html = $template.Replace('__SPEC__', (& $toJs $spec)).Replace('__DATA__', (& $toJs $visualData)).Replace('__FORMATS__', (& $toJs $formats)).Replace('__LAYOUT__', (& $toJs @($layoutWarnings))).Replace('__GENERATED__', (Get-Date -Format 'yyyy-MM-dd HH:mm'))
New-Item -ItemType Directory -Force -Path (Split-Path $OutFile -Parent) | Out-Null
[IO.File]::WriteAllText($OutFile, $html, (New-Object Text.UTF8Encoding($false)))
Write-Host "  Mockup written: $OutFile" -ForegroundColor Cyan

return [pscustomobject]@{ Ok = ($errors.Count -eq 0); OutFile = $OutFile; Errors = @($errors); LayoutWarnings = $layoutWarnings }
