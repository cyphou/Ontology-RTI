<#
.SYNOPSIS
    Pester tests for the IQ Ontology Accelerator project.
.DESCRIPTION
    Validates project structure, CSV schemas, GQL syntax, PowerShell parsing,
    and ontology definition consistency across all 6 domains.

    Run with: Invoke-Pester ./tests/Accelerator.Tests.ps1 -Output Detailed
#>

BeforeAll {
    $script:rootDir = Split-Path -Parent $PSScriptRoot
    $script:domains = @("Healthcare", "ITAsset", "ManufacturingPlant", "OilGasRefinery", "SmartBuilding", "WindTurbine")
    $script:requiredFiles = @(
        "Build-Ontology.ps1",
        "Deploy-DataAgent.ps1",
        "Deploy-KqlTables.ps1",
        "Deploy-OperationsAgent.ps1",
        "Deploy-RTIDashboard.ps1",
        "GraphQueries.gql",
        "LoadDataToTables.py"
    )
    $script:requiredFolders = @("data", "SemanticModel")
}

# Discover-time variables (Pester 5 requires these at script scope for foreach in Describe)
$rootDir = Split-Path -Parent $PSScriptRoot
$domains = @("Healthcare", "ITAsset", "ManufacturingPlant", "OilGasRefinery", "SmartBuilding", "WindTurbine")
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
    foreach ($domain in $domains) {
        Context "$domain" {
            $domainPath = Join-Path $rootDir "ontologies\$domain"

            It "domain folder exists" {
                $domainPath | Should -Exist
            }

            foreach ($file in $requiredFiles) {
                It "has $file" {
                    Join-Path $domainPath $file | Should -Exist
                }
            }

            foreach ($folder in $requiredFolders) {
                It "has $folder/ folder" {
                    Join-Path $domainPath $folder | Should -Exist
                }
            }

            It "has CSV data files" {
                $csvCount = (Get-ChildItem (Join-Path $domainPath "data") -Filter "*.csv" -ErrorAction SilentlyContinue).Count
                $csvCount | Should -BeGreaterThan 5
            }

            It "has SensorTelemetry.csv" {
                Join-Path $domainPath "data\SensorTelemetry.csv" | Should -Exist
            }
        }
    }
}

# ============================================================================
# TEST 2: CSV Schema Validation
# ============================================================================
Describe "CSV Schema Validation" {
    foreach ($domain in $domains) {
        Context "$domain" {
            $dataDir = Join-Path $rootDir "ontologies\$domain\data"
            $csvFiles = Get-ChildItem $dataDir -Filter "*.csv" -ErrorAction SilentlyContinue

            foreach ($csv in $csvFiles) {
                It "$($csv.Name) has a header row" {
                    $header = Get-Content $csv.FullName -First 1
                    $header | Should -Not -BeNullOrEmpty
                    $header | Should -Match ","
                }

                It "$($csv.Name) has data rows" {
                    $lineCount = (Get-Content $csv.FullName).Count
                    $lineCount | Should -BeGreaterThan 1
                }

                It "$($csv.Name) has no empty header columns" {
                    $header = Get-Content $csv.FullName -First 1
                    $columns = $header -split ","
                    foreach ($col in $columns) {
                        $col.Trim() | Should -Not -BeNullOrEmpty
                    }
                }
            }
        }
    }
}

# ============================================================================
# TEST 3: PowerShell Script Parse Validation
# ============================================================================
Describe "PowerShell Script Parsing" {
    Context "Root scripts" {
        $rootScripts = Get-ChildItem $rootDir -Filter "*.ps1" -File
        foreach ($script in $rootScripts) {
            It "$($script.Name) parses without errors" {
                $errors = $null
                $null = [System.Management.Automation.Language.Parser]::ParseFile($script.FullName, [ref]$null, [ref]$errors)
                $errors.Count | Should -Be 0
            }
        }
    }

    Context "Deploy scripts" {
        $deployScripts = Get-ChildItem (Join-Path $rootDir "deploy") -Filter "*.ps1" -File
        foreach ($script in $deployScripts) {
            It "$($script.Name) parses without errors" {
                $errors = $null
                $null = [System.Management.Automation.Language.Parser]::ParseFile($script.FullName, [ref]$null, [ref]$errors)
                $errors.Count | Should -Be 0
            }
        }
    }

    foreach ($domain in $domains) {
        Context "$domain scripts" {
            $domainScripts = Get-ChildItem (Join-Path $rootDir "ontologies\$domain") -Filter "*.ps1" -File
            foreach ($script in $domainScripts) {
                It "$($script.Name) parses without errors" {
                    $errors = $null
                    $null = [System.Management.Automation.Language.Parser]::ParseFile($script.FullName, [ref]$null, [ref]$errors)
                    $errors.Count | Should -Be 0
                }
            }
        }
    }
}

# ============================================================================
# TEST 4: GQL Query Validation
# ============================================================================
Describe "GQL Queries" {
    foreach ($domain in $domains) {
        Context "$domain" {
            $gqlPath = Join-Path $rootDir "ontologies\$domain\GraphQueries.gql"

            It "GraphQueries.gql exists" {
                $gqlPath | Should -Exist
            }

            It "has 20+ queries" {
                $content = Get-Content $gqlPath -Raw
                $matchCount = ([regex]::Matches($content, "(?m)^MATCH\b")).Count
                $matchCount | Should -BeGreaterOrEqual 20
            }

            It "uses /* */ comments (not #)" {
                $lines = Get-Content $gqlPath
                $hashComments = $lines | Where-Object { $_ -match "^\s*#" }
                $hashComments.Count | Should -Be 0 -Because "GQL should use /* */ comments per ISO 39075"
            }

            It "has no unclosed block comments" {
                $content = Get-Content $gqlPath -Raw
                $opens = ([regex]::Matches($content, "/\*")).Count
                $closes = ([regex]::Matches($content, "\*/")).Count
                $opens | Should -Be $closes
            }
        }
    }
}

# ============================================================================
# TEST 5: Shared Helpers Module
# ============================================================================
Describe "Shared Helpers Module" {
    $helpersPath = Join-Path $rootDir "deploy\helpers.ps1"

    It "helpers.ps1 exists" {
        $helpersPath | Should -Exist
    }

    It "exports Write-Step function" {
        $content = Get-Content $helpersPath -Raw
        $content | Should -Match "function Write-Step"
    }

    It "exports Invoke-FabricApi function" {
        $content = Get-Content $helpersPath -Raw
        $content | Should -Match "function Invoke-FabricApi"
    }

    It "exports Upload-FileToOneLake function" {
        $content = Get-Content $helpersPath -Raw
        $content | Should -Match "function Upload-FileToOneLake"
    }

    It "Deploy-GenericOntology.ps1 dot-sources helpers.ps1" {
        $generic = Get-Content (Join-Path $rootDir "deploy\Deploy-GenericOntology.ps1") -Raw
        $generic | Should -Match 'helpers\.ps1'
    }
}
