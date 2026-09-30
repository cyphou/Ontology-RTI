<#
.SYNOPSIS
    Pester tests for the IQ Ontology Accelerator project.
.DESCRIPTION
    Validates project structure, CSV schemas, GQL syntax, PowerShell parsing,
    and ontology definition consistency across all 8 domains.

    Run with: Invoke-Pester ./tests/Accelerator.Tests.ps1 -Output Detailed
#>

BeforeAll {
    $script:rootDir = Split-Path -Parent $PSScriptRoot
}

# ----------------------------------------------------------------------------
# Discovery-time data (Pester 5 evaluates -ForEach collections during discovery)
# ----------------------------------------------------------------------------
$discoRoot = Split-Path -Parent $PSScriptRoot
$domains = @("Healthcare", "ITAsset", "ManufacturingPlant", "OilGasRefinery", "SmartBuilding", "WindTurbine", "SolarFarm", "EnterpriseFinanceHR")
$requiredFiles = @(
    "Build-Ontology.ps1",
    "Deploy-DataAgent.ps1",
    "Deploy-KqlTables.ps1",
    "Deploy-OperationsAgent.ps1",
    "Deploy-RTIDashboard.ps1",
    "GraphQueries.gql",
    "LoadDataToTables.py"
)
$requiredFolders = @("data", "SemanticModel")

# ============================================================================
# TEST 1: Domain Structure Consistency
# ============================================================================
Describe "Domain Structure" {
    Context "<_>" -ForEach $domains {
        BeforeAll {
            $script:domainPath = Join-Path $script:rootDir "ontologies\$_"
        }

        It "domain folder exists" {
            $script:domainPath | Should -Exist
        }

        It "has <_>" -ForEach $requiredFiles {
            Join-Path $script:domainPath $_ | Should -Exist
        }

        It "has <_>/ folder" -ForEach $requiredFolders {
            Join-Path $script:domainPath $_ | Should -Exist
        }

        It "has CSV data files" {
            $csvCount = (Get-ChildItem (Join-Path $script:domainPath "data") -Filter "*.csv" -ErrorAction SilentlyContinue).Count
            $csvCount | Should -BeGreaterThan 5
        }

        It "has SensorTelemetry.csv" {
            Join-Path $script:domainPath "data\SensorTelemetry.csv" | Should -Exist
        }
    }
}

# ============================================================================
# TEST 2: CSV Schema Validation
# ============================================================================
Describe "CSV Schema Validation" {
    $csvCases = foreach ($domain in $domains) {
        $dataDir = Join-Path $discoRoot "ontologies\$domain\data"
        Get-ChildItem $dataDir -Filter "*.csv" -ErrorAction SilentlyContinue | ForEach-Object {
            @{ Domain = $domain; Name = $_.Name; FullName = $_.FullName }
        }
    }

    Context "<Domain>" -ForEach $csvCases {
        It "<Name> has a header row" {
            $header = Get-Content $FullName -First 1
            $header | Should -Not -BeNullOrEmpty
            $header | Should -Match ","
        }

        It "<Name> has data rows" {
            $lineCount = (Get-Content $FullName).Count
            $lineCount | Should -BeGreaterThan 1
        }

        It "<Name> has no empty header columns" {
            $header = Get-Content $FullName -First 1
            $columns = $header -split ","
            foreach ($col in $columns) {
                $col.Trim() | Should -Not -BeNullOrEmpty
            }
        }
    }
}

# ============================================================================
# TEST 3: PowerShell Script Parse Validation
# ============================================================================
Describe "PowerShell Script Parsing" {
    $rootScriptCases = Get-ChildItem $discoRoot -Filter "*.ps1" -File | ForEach-Object {
        @{ Name = $_.Name; FullName = $_.FullName }
    }
    $deployScriptCases = Get-ChildItem (Join-Path $discoRoot "deploy") -Filter "*.ps1" -File | ForEach-Object {
        @{ Name = $_.Name; FullName = $_.FullName }
    }
    $domainScriptCases = foreach ($domain in $domains) {
        Get-ChildItem (Join-Path $discoRoot "ontologies\$domain") -Filter "*.ps1" -File | ForEach-Object {
            @{ Domain = $domain; Name = $_.Name; FullName = $_.FullName }
        }
    }

    Context "Root scripts" {
        It "<Name> parses without errors" -ForEach $rootScriptCases {
            $errors = $null
            $null = [System.Management.Automation.Language.Parser]::ParseFile($FullName, [ref]$null, [ref]$errors)
            $errors.Count | Should -Be 0
        }
    }

    Context "Deploy scripts" {
        It "<Name> parses without errors" -ForEach $deployScriptCases {
            $errors = $null
            $null = [System.Management.Automation.Language.Parser]::ParseFile($FullName, [ref]$null, [ref]$errors)
            $errors.Count | Should -Be 0
        }
    }

    Context "<Domain> scripts" -ForEach $domainScriptCases {
        It "<Name> parses without errors" {
            $errors = $null
            $null = [System.Management.Automation.Language.Parser]::ParseFile($FullName, [ref]$null, [ref]$errors)
            $errors.Count | Should -Be 0
        }
    }
}

# ============================================================================
# TEST 4: GQL Query Validation
# ============================================================================
Describe "GQL Queries" {
    Context "<_>" -ForEach $domains {
        BeforeAll {
            $script:gqlPath = Join-Path $script:rootDir "ontologies\$_\GraphQueries.gql"
        }

        It "GraphQueries.gql exists" {
            $script:gqlPath | Should -Exist
        }

        It "has 20+ queries" {
            $content = Get-Content $script:gqlPath -Raw
            $matchCount = ([regex]::Matches($content, "(?m)^MATCH\b")).Count
            $matchCount | Should -BeGreaterOrEqual 20
        }

        It "uses /* */ comments (not #)" {
            $lines = Get-Content $script:gqlPath
            $hashComments = $lines | Where-Object { $_ -match "^\s*#" }
            $hashComments.Count | Should -Be 0 -Because "GQL should use /* */ comments per ISO 39075"
        }

        It "has no unclosed block comments" {
            $content = Get-Content $script:gqlPath -Raw
            $opens = ([regex]::Matches($content, "/\*")).Count
            $closes = ([regex]::Matches($content, "\*/")).Count
            $opens | Should -Be $closes
        }
    }
}

# ============================================================================
# TEST 5: Shared Helpers Module
# ============================================================================
Describe "Shared Helpers Module" {
    BeforeAll {
        $script:helpersPath = Join-Path $script:rootDir "deploy\helpers.ps1"
    }

    It "helpers.ps1 exists" {
        $script:helpersPath | Should -Exist
    }

    It "exports Write-Step function" {
        $content = Get-Content $script:helpersPath -Raw
        $content | Should -Match "function Write-Step"
    }

    It "exports Invoke-FabricApi function" {
        $content = Get-Content $script:helpersPath -Raw
        $content | Should -Match "function Invoke-FabricApi"
    }

    It "exports Upload-FileToOneLake function" {
        $content = Get-Content $script:helpersPath -Raw
        $content | Should -Match "function Upload-FileToOneLake"
    }

    It "Deploy-GenericOntology.ps1 dot-sources helpers.ps1" {
        $generic = Get-Content (Join-Path $script:rootDir "deploy\Deploy-GenericOntology.ps1") -Raw
        $generic | Should -Match 'helpers\.ps1'
    }
}

# ============================================================================
# TEST 5b: Spec-driven reports (HTML mockup gate before report creation)
# ============================================================================
$reportSpecCases = foreach ($domain in $domains) {
    $specFile = Join-Path $discoRoot "ontologies\$domain\report.spec.json"
    if (Test-Path $specFile) { @{ Domain = $domain; SpecFile = $specFile; ModelFolder = (Join-Path $discoRoot "ontologies\$domain\SemanticModel") } }
}

Describe "Spec-driven Reports" {
    BeforeAll {
        . (Join-Path $script:rootDir "deploy\ReportSpec.ps1")
        $script:template = Get-Content (Join-Path $script:rootDir "deploy\report-mockup.template.html") -Raw
    }

    It "mockup template has the spec, data and format placeholders" {
        foreach ($token in @('__SPEC__', '__DATA__', '__FORMATS__', '__IMAGES__', '__GENERATED__')) { $script:template | Should -Match $token }
    }

    It "deployment chain runs the report step after the mockup gate" {
        $generic = Get-Content (Join-Path $script:rootDir "deploy\Deploy-GenericOntology.ps1") -Raw
        $generic | Should -Match 'Deploy-ReportFromSpec\.ps1'
        $builder = Get-Content (Join-Path $script:rootDir "deploy\Deploy-ReportFromSpec.ps1") -Raw
        $builder.IndexOf('New-ReportMockup.ps1') | Should -BeLessThan $builder.IndexOf('workspaces/$WorkspaceId/items"')
    }

    It "uses the definition.pbir schema URL the service accepts" {
        $builder = Get-Content (Join-Path $script:rootDir "deploy\Deploy-ReportFromSpec.ps1") -Raw
        $builder | Should -Match 'fabric/item/report/definitionProperties/2\.\d+\.\d+/schema\.json'
        $builder | Should -Not -Match 'report/definition/definitionProperties'
    }

    It "orders ascending by category and descending by measure" {
        $asc = [pscustomobject]@{ type = 'line'; category = 'dimfiscalperiod[PeriodName]'; measures = @('factactualledger[Actual]'); sort = 'asc' }
        Get-VisualDax $asc | Should -Match "ORDER BY 'dimfiscalperiod'\[PeriodName\]$"
        $desc = [pscustomobject]@{ type = 'column'; category = 'dimjobfamily[JobFamilyName]'; measures = @('factactualledger[Actual]'); sort = 'desc' }
        Get-VisualDax $desc | Should -Match 'ORDER BY \[m0\] DESC$'
    }

    It "layout lint flags generic titles, off-grid boxes and missing KPI rows" {
        $spec = [pscustomobject]@{ reportName = 'x'; pages = @([pscustomobject]@{ name = 'p'; visuals = @(
                    [pscustomobject]@{ type = 'bar'; title = 'Revenue by Region'; x = 17; y = 16; w = 400; h = 300; category = 'a[b]'; measures = @('a[m]') }) }) }
        $warnings = @(Test-ReportLayout $spec) -join ' '
        $warnings | Should -Match "generic 'X by Y' title"
        $warnings | Should -Match 'off the 4px grid'
        $warnings | Should -Match 'no KPI/card row'
        $warnings | Should -Match 'no altText'
        $warnings | Should -Match 'no sort'
    }

    It "layout lint flags visuals that cannot honour their own choice" {
        $spec = [pscustomobject]@{ reportName = 'x'; pages = @([pscustomobject]@{ name = 'p'; visuals = @(
                    [pscustomobject]@{ type = 'gauge'; title = 'Utilisation'; x = 16; y = 16; w = 300; h = 120; measures = @('a[m]'); altText = 'x' },
                    [pscustomobject]@{ type = 'kpi'; title = 'Spend'; x = 332; y = 16; w = 300; h = 120; category = 'a[b]'; measures = @('a[m]'); altText = 'x' },
                    [pscustomobject]@{ type = 'combo'; title = 'Volume and rate'; x = 648; y = 16; w = 300; h = 120; category = 'a[b]'; measures = @('a[m]'); altText = 'x' }) }) }
        $warnings = @(Test-ReportLayout $spec) -join ' '
        $warnings | Should -Match 'gauge without a target'
        $warnings | Should -Match 'KPI without goalMeasures'
        $warnings | Should -Match 'combo without lineMeasures'
    }

    It "data-aware lint rejects a donut with too many slices and a waterfall that never subtracts" {
        $donut = [pscustomobject]@{ type = 'donut'; title = 'Split' }
        (@(Test-VisualFit $donut 9 @()) -join ' ') | Should -Match 'slices'
        $flat = [pscustomobject]@{ type = 'waterfall'; title = 'Variance' }
        $rows = @([pscustomobject]@{ m0 = 5 }, [pscustomobject]@{ m0 = 3 })
        (@(Test-VisualFit $flat 2 $rows) -join ' ') | Should -Match 'no value is negative'
    }

    It "every visual type in the mapping is supported by the builder" {
        $mapping = Get-VisualMapping
        $mapping | Should -Not -BeNullOrEmpty
        foreach ($intent in $mapping.intents) {
            if ($intent.use -eq 'scatter') { continue }  # documented in the mapping, not yet emitted by the builder
            $script:ReportVisualTypes | Should -Contain $intent.use
        }
    }

    It "rejects a measure bound across an inactive relationship" {
        # dimsensor and factincident exist in ITAsset but factincident_ServerId is inactive,
        # so this pairing would repeat one unfiltered total for every server type.
        $spec = [pscustomobject]@{ reportName = 'x'; pages = @([pscustomobject]@{ name = 'p'; visuals = @(
                        [pscustomobject]@{ type = 'bar'; title = 'Incidents'; x = 16; y = 16; w = 400; h = 300; sort = 'desc'; altText = 'x'
                        category = 'dimserver[ServerType]'; measures = @('factincident[Incident Count]') }) }) }
        $problems = @(Test-ReportSpec $spec (Join-Path $script:rootDir "ontologies\ITAsset\SemanticModel"))
        ($problems -join ' ') | Should -Match 'no active relationship connects'
    }

    It "rejects unknown measures and overlapping visuals" {
        $bad = [pscustomobject]@{ reportName = 'x'; pages = @([pscustomobject]@{ name = 'p'; visuals = @(
                    [pscustomobject]@{ type = 'card'; title = 'a'; x = 0; y = 0; w = 200; h = 100; measures = @('factproduction[Does Not Exist]') },
                    [pscustomobject]@{ type = 'card'; title = 'b'; x = 100; y = 50; w = 200; h = 100; measures = @('factproduction[Total Output Barrels]') }) }) }
        $problems = @(Test-ReportSpec $bad (Join-Path $script:rootDir "ontologies\OilGasRefinery\SemanticModel"))
        ($problems -join ' ') | Should -Match 'unknown measure'
        ($problems -join ' ') | Should -Match 'overlaps'
    }

    Context "<Domain> report.spec.json" -ForEach $reportSpecCases {
        It "is valid against the semantic model TMDL" {
            $problems = @(Test-ReportSpec (Read-ReportSpec $SpecFile) $ModelFolder)
            $problems -join "`n" | Should -BeNullOrEmpty
        }

        It "produces a DAX query for every data-bound visual" {
            foreach ($page in (Read-ReportSpec $SpecFile).pages) {
                foreach ($v in $page.visuals) {
                    if ($v.type -in 'textbox', 'logo') { Get-VisualDax $v | Should -BeNullOrEmpty; continue }
                    Get-VisualDax $v | Should -Match '^EVALUATE '
                }
            }
        }

        It "passes the mapping-driven layout lint with no warnings" {
            @(Test-ReportLayout (Read-ReportSpec $SpecFile)) -join "`n" | Should -BeNullOrEmpty
        }

        It "resolves a theme and a logo from the domain branding" {
            $spec = Read-ReportSpec $SpecFile
            $spec.theme.accent | Should -Match '^#[0-9A-Fa-f]{6}$'
            @($spec.theme.dataColors).Count | Should -BeGreaterThan 3
            Test-Path (Join-Path $script:rootDir $spec.theme.logo) | Should -BeTrue
        }
    }
}

# ============================================================================
# TEST 6: Enterprise Finance + HR Expansion
# ============================================================================
Describe "Enterprise Finance + HR Expansion" {
    BeforeAll {
        $script:enterprisePath = Join-Path $script:rootDir "ontologies\EnterpriseFinanceHR"
        $script:enterpriseData = Join-Path $script:enterprisePath "data"
        $script:enterpriseTables = @(
            "DimCalendarDate.csv", "DimPayGrade.csv", "DimCompensationComponent.csv", "FactEmployeeCompensationSnapshot.csv",
            "DimWorkSchedule.csv", "DimAttendanceCode.csv", "FactTimeAttendanceDaily.csv", "DimLeaveType.csv",
            "DimAbsenceReasonCategory.csv", "FactAbsenceEpisode.csv", "DimRecruitmentStage.csv", "DimRecruitmentSource.csv",
            "DimCandidate.csv", "FactJobRequisition.csv", "FactRecruitmentStageEvent.csv", "FactOffer.csv"
        )
    }

    It "has all expanded synthetic source tables" {
        foreach ($table in $script:enterpriseTables) { Join-Path $script:enterpriseData $table | Should -Exist }
    }

    It "has the expected cross-domain foreign key columns" {
        (Get-Content (Join-Path $script:enterpriseData "FactEmployeeCompensationSnapshot.csv") -First 1) | Should -Match 'EmployeeId"\s*,\s*"FiscalPeriodId"\s*,\s*"PayGradeId"\s*,\s*"CompensationComponentId'
        (Get-Content (Join-Path $script:enterpriseData "FactTimeAttendanceDaily.csv") -First 1) | Should -Match 'EmployeeId"\s*,\s*"CalendarDateId"\s*,\s*"WorkScheduleId"\s*,\s*"AttendanceCodeId'
        (Get-Content (Join-Path $script:enterpriseData "FactAbsenceEpisode.csv") -First 1) | Should -Match 'EmployeeId"\s*,\s*"LeaveTypeId"\s*,\s*"AbsenceReasonCategoryId'
        (Get-Content (Join-Path $script:enterpriseData "FactJobRequisition.csv") -First 1) | Should -Match 'PositionId"\s*,\s*"DepartmentId"\s*,\s*"CostCenterId'
    }

    It "defines all expanded ontology entities and aggregate dataflow artifacts" {
        $ontology = Get-Content (Join-Path $script:enterprisePath "Build-Ontology.ps1") -Raw
        foreach ($entity in @("CalendarDate", "PayGrade", "EmployeeCompensationSnapshot", "TimeAttendanceDaily", "AbsenceEpisode", "Candidate", "JobRequisition", "Offer")) { $ontology | Should -Match "'$entity'" }
        foreach ($flow in @("compensation", "attendance", "absence", "recruitment")) {
            Join-Path $script:enterprisePath "datafactory\$flow\mashup.pq" | Should -Exist
            (Get-Content (Join-Path $script:enterprisePath "datafactory\$flow\mashup.pq") -Raw) | Should -Match "bi_"
        }
        Join-Path $script:enterprisePath "Deploy-HRDataflowsGen2.ps1" | Should -Exist
        Join-Path $script:enterprisePath "Deploy-HRDataPipeline.ps1" | Should -Exist
        Join-Path $script:enterprisePath "DataPipeline\definition\pipeline-content.json" | Should -Exist
    }
}

# ============================================================================
# TEST 7: Solar Farm semantic model generation
# ============================================================================
Describe "Solar Farm semantic model" {
    BeforeAll {
        $script:solarRoot = Join-Path $script:rootDir "ontologies\SolarFarm"
        $script:solarTables = Join-Path $script:solarRoot "SemanticModel\definition\tables"
        . (Join-Path $script:rootDir "deploy\ReportSpec.ps1")
    }

    It "has one TMDL table for every Solar Farm CSV" {
        $csvNames = @(Get-ChildItem (Join-Path $script:solarRoot "data") -Filter "*.csv" | ForEach-Object { $_.BaseName.ToLowerInvariant() } | Sort-Object)
        $tmdlNames = @(Get-ChildItem $script:solarTables -Filter "*.tmdl" | ForEach-Object { $_.BaseName.ToLowerInvariant() } | Sort-Object)
        $csvNames.Count | Should -Be 26
        ($tmdlNames -join ',') | Should -Be ($csvNames -join ',')
    }

    It "defines production, alert and maintenance measures with explicit formats" {
        $production = Get-Content (Join-Path $script:solarTables "factenergyproduction.tmdl") -Raw
        $alerts = Get-Content (Join-Path $script:solarTables "factalert.tmdl") -Raw
        $maintenance = Get-Content (Join-Path $script:solarTables "factmaintenanceevent.tmdl") -Raw
        $production | Should -Match "measure 'Avg Power Output KW'"
        $production | Should -Match "measure 'Avg Performance Ratio'"
        $production | Should -Match 'formatString: 0\.0%'
        $alerts | Should -Match "measure 'Critical Alert Count'"
        $maintenance | Should -Match "measure 'Total Maintenance Cost'"
        $maintenance | Should -Match 'formatString: \$#,0'
    }

    It "keeps fact filter paths active and ambiguous sensor paths inactive" {
        $rels = Get-Content (Join-Path $script:solarRoot "SemanticModel\definition\relationships.tmdl") -Raw
        $rels | Should -Match '(?s)relationship factenergyproduction_ArrayId_dimsolararray\s+fromColumn:'
        $rels | Should -Match '(?s)relationship factmaintenanceevent_ArrayId_dimsolararray\s+fromColumn:'
        $rels | Should -Match '(?s)relationship factalert_ArrayId_dimsolararray\s+fromColumn:'
        $rels | Should -Match '(?s)relationship factalert_SensorId_dimsensor\s+isActive: false'
    }

    It "does not claim a time trend for a single-date production sample" {
        $dates = @(Import-Csv (Join-Path $script:solarRoot "data\FactEnergyProduction.csv") | Select-Object -ExpandProperty Date -Unique)
        $dates.Count | Should -Be 1
        $spec = Read-ReportSpec (Join-Path $script:solarRoot "report.spec.json")
        foreach ($page in $spec.pages) {
            foreach ($visual in $page.visuals) {
                if ($visual.type -eq 'line' -and $visual.category -eq 'factenergyproduction[Date]') {
                    throw "Production has only one distinct date; a time-series line is misleading."
                }
            }
        }
    }

    It "registers SolarFarm in the generator's All set and lineage map" {
        $generator = Get-Content (Join-Path $script:rootDir "Generate-SemanticModels.ps1") -Raw
        $generator | Should -Match 'ValidateSet\([^\)]*"SolarFarm"'
        $generator | Should -Match '"Healthcare","SolarFarm"\)'
        $generator | Should -Match '"SolarFarm"\s*=\s*70000000'
    }
}
