<#
.SYNOPSIS
    Pester tests for the IQ Ontology Accelerator project.
.DESCRIPTION
    Validates project structure, CSV schemas, GQL syntax, PowerShell parsing,
    and ontology definition consistency across all 7 domains.

    Run with: Invoke-Pester ./tests/Accelerator.Tests.ps1 -Output Detailed
#>

BeforeAll {
    $script:rootDir = Split-Path -Parent $PSScriptRoot
}

# ----------------------------------------------------------------------------
# Discovery-time data (Pester 5 evaluates -ForEach collections during discovery)
# ----------------------------------------------------------------------------
$discoRoot = Split-Path -Parent $PSScriptRoot
$domains = @("Healthcare", "ITAsset", "ManufacturingPlant", "OilGasRefinery", "SmartBuilding", "WindTurbine", "SolarFarm")
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
