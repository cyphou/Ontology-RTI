# GitHub Projects Import Guide (Wind Turbine Rayfin)

Use this file to import the roadmap backlog into a GitHub Project in one pass.

## Import source

1. docs/WIND_TURBINE_RAYFIN_GITHUB_PROJECT_IMPORT.csv

## Recommended project setup

Before importing, create these single-select fields in your GitHub Project if they do not already exist:

1. Status with options: Todo, In Progress, Done
2. Priority with options: P0, P1
3. Sprint with options: Sprint 0, Sprint 1, Sprint 2, Sprint 3, Sprint 4, Sprint 5
4. OwnerRole as text or single-select
5. EstimateDays as number

Milestone and Labels can map to native GitHub item fields.

## Field mapping during CSV import

Map CSV columns to project fields as follows:

1. Title -> Title
2. Body -> Body
3. Status -> Status
4. Labels -> Labels
5. Repository -> Repository
6. Milestone -> Milestone
7. Priority -> Priority
8. EstimateDays -> EstimateDays
9. OwnerRole -> OwnerRole
10. Sprint -> Sprint

## Expected initial state after import

1. All Sprint 1 items are set to In Progress.
2. Sprint 0 and Sprint 2-5 items are set to Todo.
3. Repository is pre-set to cyphou/Ontology-RTI for each item.

## Quick validation checklist

1. Filter Sprint = Sprint 1 and confirm all are In Progress.
2. Filter Milestone = M1 and verify S0 and S1 scope is complete.
3. Group by Sprint and confirm item counts match roadmap phases.

## Optional follow-up automations

1. Auto-set Status to Done when linked PR is merged.
2. Create saved views by Sprint and by OwnerRole.
3. Add an iteration field if you want date-bound sprint windows in Project views.
