param(
    [int]$MaxAttempts = 12,
    [int]$DelaySeconds = 20,
    [switch]$SkipBuild = $true
)

$ErrorActionPreference = "Stop"

if ($MaxAttempts -lt 1) {
    throw "MaxAttempts must be >= 1."
}

if ($DelaySeconds -lt 1) {
    throw "DelaySeconds must be >= 1."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$logDir = Join-Path $repoRoot "rayfin\.deploy-logs"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$logPath = Join-Path $logDir "static-deploy-retry-$stamp.log"
$activityPath = Join-Path $logDir "static-deploy-activityids-$stamp.txt"

function Write-Log {
    param([string]$Message)
    $line = "[$(Get-Date -Format o)] $Message"
    Write-Host $line
    Add-Content -Path $logPath -Value $line
}

Write-Log "Starting static deploy retry loop."
Write-Log "MaxAttempts=$MaxAttempts DelaySeconds=$DelaySeconds SkipBuild=$SkipBuild"
Write-Log "Log file: $logPath"

for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
    Write-Log "Attempt $attempt of $MaxAttempts"

    $args = @("rayfin", "up", "staticapp", "deploy")
    if ($SkipBuild) {
        $args += "--skip-build"
    }
    $args += "--verbose"

    $output = & npx @args 2>&1
    $exitCode = $LASTEXITCODE

    $outputText = ($output | Out-String)
    Add-Content -Path $logPath -Value $outputText

    $ids = [regex]::Matches($outputText, "RootActivityId:\s*([0-9a-fA-F-]{36})") | ForEach-Object { $_.Groups[1].Value }
    if ($ids.Count -gt 0) {
        $ids | ForEach-Object { Add-Content -Path $activityPath -Value $_ }
        Write-Log "Captured RootActivityId: $($ids[-1])"
    }

    if ($exitCode -eq 0) {
        Write-Log "Static deploy succeeded on attempt $attempt."
        Write-Host "Success. Logs: $logPath"
        if (Test-Path $activityPath) {
            Write-Host "Activity IDs: $activityPath"
        }
        exit 0
    }

    Write-Log "Attempt $attempt failed (exit code $exitCode)."

    if ($attempt -lt $MaxAttempts) {
        Write-Log "Waiting $DelaySeconds seconds before retry..."
        Start-Sleep -Seconds $DelaySeconds
    }
}

Write-Log "Static deploy failed after $MaxAttempts attempts."
Write-Host "Failed. Logs: $logPath"
if (Test-Path $activityPath) {
    Write-Host "Activity IDs: $activityPath"
}
exit 1
