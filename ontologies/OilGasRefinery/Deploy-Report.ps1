<#
.SYNOPSIS
  Deploys the Refinery Operations report from report.spec.json (mockup gate first).
.DESCRIPTION
  Thin wrapper over deploy/Deploy-ReportFromSpec.ps1. Customize the report by editing
  report.spec.json (pages, visuals, positions, measures, theme), then run with -MockupOnly to
  preview on live data before deploying.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$WorkspaceId,
    [Parameter(Mandatory = $true)][string]$SemanticModelId,
    [string]$PbipOutDir = '',
    [switch]$MockupOnly,
    [switch]$Force
)

$builder = Join-Path (Split-Path $PSScriptRoot -Parent | Split-Path -Parent) 'deploy\Deploy-ReportFromSpec.ps1'
$params = @{ SpecPath = (Join-Path $PSScriptRoot 'report.spec.json'); WorkspaceId = $WorkspaceId; SemanticModelId = $SemanticModelId }
if ($PbipOutDir) { $params.PbipOutDir = $PbipOutDir }
if ($MockupOnly) { $params.MockupOnly = $true }
if ($Force) { $params.Force = $true }
& $builder @params
