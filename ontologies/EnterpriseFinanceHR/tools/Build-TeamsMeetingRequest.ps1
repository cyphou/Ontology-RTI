<#
.SYNOPSIS
  Builds the Microsoft Graph (WorkIQ) request body for scheduling a Teams review
  meeting that presents the cowork insights briefing. Prints ready-to-send JSON;
  does not call Graph directly (run via the workiq MCP tools from the agent).
#>
[CmdletBinding()]
param(
    [string]$InsightsFile = 'C:\GitHub Project\OntologyAccelerator\artifacts\cowork-insights.json',
    [string]$Subject = 'Enterprise Finance + HR - AI Insights Review',
    [int]$DaysFromNow = 2,
    [string]$StartTime = '10:00',
    [int]$DurationMinutes = 30
)

$ErrorActionPreference = 'Stop'
$insights = Get-Content $InsightsFile -Raw | ConvertFrom-Json

$startDate = (Get-Date).Date.AddDays($DaysFromNow)
$startHour, $startMin = $StartTime -split ':'
$start = $startDate.AddHours([int]$startHour).AddMinutes([int]$startMin)
$end = $start.AddMinutes($DurationMinutes)

$questionsHtml = ($insights.questions.PSObject.Properties | ForEach-Object {
        "<li><b>$($_.Name)</b><br/>$($_.Value)</li>"
    }) -join "`n"

$bodyHtml = @"
<h3>Enterprise Finance + HR &mdash; AI Insights Briefing</h3>
<p>Generated automatically from the live Direct Lake semantic model on $($insights.generatedAt).</p>
<ul>
$questionsHtml
</ul>
<p>Deck attached/linked: EnterpriseFinanceHR-Insights.pptx</p>
"@

$eventBody = [ordered]@{
    subject             = $Subject
    isOnlineMeeting     = $true
    onlineMeetingProvider = 'teamsForBusiness'
    start               = [ordered]@{ dateTime = $start.ToString('yyyy-MM-ddTHH:mm:ss'); timeZone = 'Romance Standard Time' }
    end                 = [ordered]@{ dateTime = $end.ToString('yyyy-MM-ddTHH:mm:ss'); timeZone = 'Romance Standard Time' }
    body                = [ordered]@{ contentType = 'HTML'; content = $bodyHtml }
}

$eventBody | ConvertTo-Json -Depth 10
