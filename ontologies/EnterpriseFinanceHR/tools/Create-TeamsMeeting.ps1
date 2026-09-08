<#
.SYNOPSIS
  Creates the Teams review meeting for the Enterprise Finance + HR cowork insights,
  for manual execution (uses the Microsoft Graph PowerShell SDK, not the agent's
  workiq tool). Run this yourself after reviewing/editing artifacts/cowork-insights.json.

.DESCRIPTION
  Reads artifacts/cowork-insights.json (produced by Generate-CoworkInsights.ps1),
  builds an HTML meeting body from the Q&A, and creates a Teams online meeting event
  on your calendar via Microsoft Graph (POST /me/events).

.EXAMPLE
  Install-Module Microsoft.Graph.Calendar -Scope CurrentUser   # one-time
  .\tools\Create-TeamsMeeting.ps1 -DaysFromNow 2 -StartTime '10:00'
#>
[CmdletBinding()]
param(
    [string]$InsightsFile = 'C:\GitHub Project\OntologyAccelerator\artifacts\cowork-insights.json',
    [string]$Subject = 'Enterprise Finance + HR - AI Insights Review',
    [int]$DaysFromNow = 2,
    [string]$StartTime = '10:00',
    [int]$DurationMinutes = 30,
    [string]$TimeZone = 'Romance Standard Time',
    [string[]]$AttendeeEmails = @()
)

$ErrorActionPreference = 'Stop'

if (-not (Get-Module -ListAvailable -Name Microsoft.Graph.Calendar)) {
    Write-Host 'Installing Microsoft.Graph.Calendar (one-time, current user scope)...' -ForegroundColor Yellow
    Install-Module Microsoft.Graph.Calendar -Scope CurrentUser -Force
}
Import-Module Microsoft.Graph.Calendar

Write-Host 'Connecting to Microsoft Graph (Calendars.ReadWrite)...' -ForegroundColor Cyan
Connect-MgGraph -Scopes 'Calendars.ReadWrite' -NoWelcome

$insights = Get-Content $InsightsFile -Raw | ConvertFrom-Json

$startDate = (Get-Date).Date.AddDays($DaysFromNow)
$startHour, $startMin = $StartTime -split ':'
$start = $startDate.AddHours([int]$startHour).AddMinutes([int]$startMin)
$end = $start.AddMinutes($DurationMinutes)

$questionsHtml = ($insights.questions.PSObject.Properties | ForEach-Object {
        "<li><b>$($_.Name)</b><br/>$($_.Value)</li>"
    }) -join ''

$bodyHtml = "<h3>Enterprise Finance + HR - AI Insights Briefing</h3>" +
"<p>Generated automatically from the live Direct Lake semantic model on $($insights.generatedAt).</p>" +
"<ul>$questionsHtml</ul>" +
"<p>Deck: EnterpriseFinanceHR-Insights.pptx (attach manually before sending invites).</p>"

$attendees = @()
foreach ($email in $AttendeeEmails) {
    $attendees += @{ emailAddress = @{ address = $email }; type = 'required' }
}

$params = @{
    subject               = $Subject
    isOnlineMeeting       = $true
    onlineMeetingProvider = 'teamsForBusiness'
    start                 = @{ dateTime = $start.ToString('yyyy-MM-ddTHH:mm:ss'); timeZone = $TimeZone }
    end                   = @{ dateTime = $end.ToString('yyyy-MM-ddTHH:mm:ss'); timeZone = $TimeZone }
    body                  = @{ contentType = 'HTML'; content = $bodyHtml }
}
if ($attendees.Count -gt 0) { $params.attendees = $attendees }

$event = New-MgUserEvent -UserId 'me' -BodyParameter $params

Write-Host "Meeting created: $($event.Subject)" -ForegroundColor Green
Write-Host "Start: $($event.Start.DateTime) $($event.Start.TimeZone)" -ForegroundColor Green
Write-Host "Teams join URL: $($event.OnlineMeeting.JoinUrl)" -ForegroundColor Cyan
Write-Host "Calendar link: $($event.WebLink)" -ForegroundColor Cyan

Disconnect-MgGraph | Out-Null
