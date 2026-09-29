param(
    [string]$Root = (Split-Path $PSScriptRoot -Parent),
    [string]$Domain
)
<#
.SYNOPSIS
  Generates the report logo lockup (icon + domain name) for each ontology domain.
.DESCRIPTION
  Reads deploy/domain-branding.json so the logo, the report theme and the mockup share one
  source of colour. Rasterises through headless Edge because Power BI's image visual expects
  a raster image, and Edge ships with Windows so this adds no dependency.
#>
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Drawing
$branding = Get-Content (Join-Path $PSScriptRoot 'domain-branding.json') -Raw | ConvertFrom-Json
$edge = @("${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe", "$env:ProgramFiles\Microsoft\Edge\Application\msedge.exe") |
    Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $edge) { throw 'Microsoft Edge not found; it is required to rasterise the logo.' }

# Headless Edge offsets the capture region, so render on an oversized canvas and crop to the
# content instead of trusting the window size.
function Save-CroppedPng([string]$Path, [int]$Pad = 12) {
    $src = [Drawing.Bitmap]::FromFile($Path)
    try {
        $minX = $src.Width; $minY = $src.Height; $maxX = -1; $maxY = -1
        for ($y = 0; $y -lt $src.Height; $y++) {
            for ($x = 0; $x -lt $src.Width; $x++) {
                $p = $src.GetPixel($x, $y)
                if ($p.A -gt 8 -and ($p.R -lt 248 -or $p.G -lt 248 -or $p.B -lt 248)) {
                    if ($x -lt $minX) { $minX = $x }; if ($x -gt $maxX) { $maxX = $x }
                    if ($y -lt $minY) { $minY = $y }; if ($y -gt $maxY) { $maxY = $y }
                }
            }
        }
        if ($maxX -lt 0) { return $false }
        $minX = [Math]::Max(0, $minX - $Pad); $minY = [Math]::Max(0, $minY - $Pad)
        $maxX = [Math]::Min($src.Width - 1, $maxX + $Pad); $maxY = [Math]::Min($src.Height - 1, $maxY + $Pad)
        $rect = New-Object Drawing.Rectangle $minX, $minY, ($maxX - $minX + 1), ($maxY - $minY + 1)
        $crop = $src.Clone($rect, $src.PixelFormat)
        $bytes = New-Object IO.MemoryStream
        $crop.Save($bytes, [Drawing.Imaging.ImageFormat]::Png)
        $crop.Dispose()
        $src.Dispose(); $src = $null
        [IO.File]::WriteAllBytes($Path, $bytes.ToArray())
        return $true
    } finally { if ($src) { $src.Dispose() } }
}

New-Item -ItemType Directory -Force -Path (Join-Path $Root 'assets\logos') | Out-Null
$profileDir = Join-Path $env:TEMP 'edge-logo-shot'

$names = if ($Domain) { @($Domain) } else { $branding.domains.PSObject.Properties.Name }
foreach ($name in $names) {
    $b = $branding.domains.$name
    $iconPath = Join-Path $Root ($b.icon -replace '/', '\')
    if (-not (Test-Path $iconPath)) { Write-Warning "Icon missing for ${name}: $iconPath"; continue }
    $svg = (Get-Content $iconPath -Raw) -replace '<\?xml[^>]*\?>', ''
    $svg = $svg -replace '<svg', '<svg width="96" height="96" preserveAspectRatio="xMidYMid meet"'
    $html = @"
<!doctype html><meta charset="utf-8">
<body style="margin:0;background:#FFFFFF;font-family:'Segoe UI',system-ui,sans-serif">
  <div style="display:flex;align-items:center;gap:20px;padding:60px 24px 60px 24px">
    <div style="flex:0 0 96px;height:96px;display:flex">$svg</div>
    <div>
      <div style="font-size:30px;font-weight:600;color:$($b.accent);letter-spacing:-0.4px;line-height:1.2">$([Net.WebUtility]::HtmlEncode($b.displayName))</div>
      <div style="font-size:16px;color:#5C6B7A;margin-top:4px">$([Net.WebUtility]::HtmlEncode($b.tagline))</div>
    </div>
  </div>
</body>
"@
    $htmlFile = Join-Path $env:TEMP "logo-$name.html"
    [IO.File]::WriteAllText($htmlFile, $html, (New-Object Text.UTF8Encoding($false)))
    $png = Join-Path $Root ($b.logo -replace '/', '\')
    if (Test-Path $png) { Remove-Item $png -Force }
    & $edge --headless=new --disable-gpu --no-sandbox --user-data-dir="$profileDir" --hide-scrollbars `
        --run-all-compositor-stages-before-draw --virtual-time-budget=4000 `
        --screenshot="$png" --window-size=900,340 "file:///$($htmlFile -replace '\\','/')" 2>&1 | Out-Null
    # Edge returns before the screenshot lands on disk.
    for ($i = 0; $i -lt 40 -and -not (Test-Path $png); $i++) { Start-Sleep -Milliseconds 250 }
    if ((Test-Path $png) -and (Save-CroppedPng $png)) {
        $img = [Drawing.Bitmap]::FromFile($png); $dim = "$($img.Width)x$($img.Height)"; $img.Dispose()
        "{0,-24} {1,7:N0} b  {2,-10} {3}" -f $name, (Get-Item $png).Length, $dim, $b.logo
    } else { Write-Warning "Failed to render logo for $name" }
}

