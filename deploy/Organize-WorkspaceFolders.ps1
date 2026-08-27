$ErrorActionPreference = "Stop"
$tok = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com" -WarningAction SilentlyContinue).Token
$ws = "c4e0ab47-88d2-452d-ac98-ad101b574cf3"
$h = @{ Authorization = "Bearer $tok"; "Content-Type" = "application/json" }
$base = "https://api.fabric.microsoft.com/v1/workspaces/$ws"

$folderNames = @("Refinery Worldwide", "Wind Turbine", "Solar France", "Contoso Energy")

$existing = @{}
(Invoke-RestMethod -Uri "$base/folders" -Headers $h).value | ForEach-Object { $existing[$_.displayName] = $_.id }
$folderId = @{}
foreach ($fn in $folderNames) {
    if ($existing.ContainsKey($fn)) { $folderId[$fn] = $existing[$fn]; "reuse folder: $fn" }
    else {
        $f = Invoke-RestMethod -Method Post -Uri "$base/folders" -Headers $h -Body (@{ displayName = $fn } | ConvertTo-Json)
        $folderId[$fn] = $f.id; "created folder: $fn"
    }
}

function Get-Demo($name) {
    $n = $name.ToLower()
    if ($n -match "refinery|oilgas") { return "Refinery Worldwide" }
    if ($n -match "wind")            { return "Wind Turbine" }
    if ($n -match "solar")           { return "Solar France" }
    if ($n -match "contoso" -or $name -match "^\d\d_" -or $name -in @("BronzeLH","SilverLH","GoldLH","data-app")) { return "Contoso Energy" }
    return $null
}

$items = (Invoke-RestMethod -Uri "$base/items" -Headers $h).value
$moved = 0; $skipped = 0; $failed = 0; $unmatched = @()
foreach ($it in $items) {
    if ($it.type -in @("SQLEndpoint", "KQLDatabase")) { $skipped++; continue }
    $demo = Get-Demo $it.displayName
    if (-not $demo) { $unmatched += "$($it.displayName) [$($it.type)]"; continue }
    $target = $folderId[$demo]
    if ($it.folderId -eq $target) { $skipped++; continue }
    try {
        Invoke-RestMethod -Method Post -Uri "$base/items/$($it.id)/move" -Headers $h -Body (@{ targetFolderId = $target } | ConvertTo-Json) | Out-Null
        "  moved: $($it.displayName) [$($it.type)] -> $demo"; $moved++
    }
    catch {
        "  FAILED: $($it.displayName) [$($it.type)] -> $demo : $($_.Exception.Message)"; $failed++
    }
}
""
"=== SUMMARY ==="
"moved=$moved  skipped(child/already)=$skipped  failed=$failed"
if ($unmatched.Count) { "unmatched (left at root): " + ($unmatched -join ", ") }
