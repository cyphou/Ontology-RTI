$ErrorActionPreference = "Stop"
$tok = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com" -WarningAction SilentlyContinue).Token
$ws = "c4e0ab47-88d2-452d-ac98-ad101b574cf3"
$h = @{ Authorization = "Bearer $tok"; "Content-Type" = "application/json" }
$base = "https://api.fabric.microsoft.com/v1/workspaces/$ws"

$folders = (Invoke-RestMethod -Uri "$base/folders" -Headers $h).value
$contoso = ($folders | Where-Object { $_.displayName -eq "Contoso Energy" }).id

$subNames = @("01 Data", "02 Transform", "03 Analytics", "04 Writeback")
$sub = @{}
foreach ($s in $subNames) {
    $existingSub = $folders | Where-Object { $_.displayName -eq $s -and $_.parentFolderId -eq $contoso }
    if ($existingSub) { $sub[$s] = $existingSub.id; "reuse subfolder: $s" }
    else {
        $f = Invoke-RestMethod -Method Post -Uri "$base/folders" -Headers $h -Body (@{ displayName = $s; parentFolderId = $contoso } | ConvertTo-Json)
        $sub[$s] = $f.id; "created subfolder: Contoso Energy/$s"
    }
}

$stage = @{
    "BronzeLH" = "01 Data"; "01_BronzeToSilver" = "01 Data"; "05_EventSimulator" = "01 Data"; "ContosoEnergy-ETL" = "01 Data"; "ContosoEnergy-Pipeline" = "01 Data"
    "SilverLH" = "02 Transform"; "GoldLH" = "02 Transform"; "02_WebEnrichment" = "02 Transform"; "03_SilverToGold" = "02 Transform"; "04_Forecasting" = "02 Transform"; "ContosoEnergy_Forecasting" = "02 Transform"
    "ContosoEnergyModel" = "03 Analytics"; "ContosoEnergy-Analytics" = "03 Analytics"; "ContosoEnergy-Forecasting" = "03 Analytics"; "ContosoEnergy-HTAP" = "03 Analytics"; "06_DiagnosticCheck" = "03 Analytics"
    "07_WritebackSetup" = "04 Writeback"; "08_WritebackAPI" = "04 Writeback"; "09_SQLDatabaseSetup" = "04 Writeback"; "ContosoEnergyWritebackDB" = "04 Writeback"; "ContosoEnergyWritebackModel" = "04 Writeback"; "ContosoEnergyWritebackUDF" = "04 Writeback"; "ContosoEnergy-Writeback" = "04 Writeback"
}

$items = (Invoke-RestMethod -Uri "$base/items" -Headers $h).value
$moved = 0; $failed = 0
foreach ($it in $items) {
    if ($it.type -in @("SQLEndpoint", "KQLDatabase")) { continue }
    if ($it.folderId -ne $contoso) { continue }
    $st = $stage[$it.displayName]
    if (-not $st) { continue }
    try {
        Invoke-RestMethod -Method Post -Uri "$base/items/$($it.id)/move" -Headers $h -Body (@{ targetFolderId = $sub[$st] } | ConvertTo-Json) | Out-Null
        "  moved: $($it.displayName) -> Contoso Energy/$st"; $moved++
    }
    catch { "  FAILED: $($it.displayName) -> $st : $($_.Exception.Message)"; $failed++ }
}

$deleted = 0
foreach ($f in ($folders | Where-Object { $_.displayName -in $subNames -and -not $_.parentFolderId })) {
    try {
        Invoke-RestMethod -Method Delete -Uri "$base/folders/$($f.id)" -Headers $h | Out-Null
        "  deleted stale top-level folder: $($f.displayName)"; $deleted++
    }
    catch { "  could not delete $($f.displayName): $($_.Exception.Message)" }
}
""
"redistributed=$moved  failed=$failed  deletedStaleFolders=$deleted"
